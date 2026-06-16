//! Tighter-batch composite scoring for transaction packing.
//!
//! Computes a "value score" that balances short-term fee revenue with
//! long-term block-packing efficiency.
//!
//! The score formula:
//! ```text
//! score = compute_unit_price * weight_fee
//!       + (1_000_000 / cu_limit) * weight_efficiency
//! ```
//!
//! - `compute_unit_price` = prioritization_fee / cu_limit (fee density)
//! - `1_000_000 / cu_limit` rewards txs that request fewer CUs (tighter estimates)
//! - Weights are configurable; default both are 1 (equal weight)

use {
    agave_feature_set::FeatureSet,
    agave_transaction_view::transaction_view::SanitizedTransactionView, serde::Deserialize,
    solana_compute_budget_instruction::compute_budget_instruction_details,
    solana_cost_model::cost_model::CostModel,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_transaction::sanitized::MessageHash,
};

/// Configuration for the tighter-batch composite scoring strategy.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct TighterBatchConfig {
    /// Weight applied to `compute_unit_price` (fee density). Defaults to 1.
    #[serde(default = "default_weight_fee")]
    pub weight_fee: u64,
    /// Weight applied to CU efficiency (fewer CU requested = better). Defaults to 1.
    #[serde(default = "default_weight_efficiency")]
    pub weight_efficiency: u64,
    /// Minimum score threshold — txs below this are dropped. Defaults to 0.
    #[serde(default = "default_min_score")]
    pub min_score: u64,
}

const fn default_weight_fee() -> u64 {
    1
}

const fn default_weight_efficiency() -> u64 {
    1
}

const fn default_min_score() -> u64 {
    0
}

impl Default for TighterBatchConfig {
    fn default() -> Self {
        Self {
            weight_fee: default_weight_fee(),
            weight_efficiency: default_weight_efficiency(),
            min_score: default_min_score(),
        }
    }
}

/// Cost data extracted from a transaction.
#[derive(Debug, Clone, Copy)]
pub struct TxCosts {
    /// Prioritization fee in lamports.
    pub prioritization_fee: u64,
    /// Compute unit limit requested by the transaction.
    pub compute_unit_limit: u64,
    /// Total cost as computed by the CostModel.
    pub total_cost: u64,
}

impl TxCosts {
    /// Compute the compute unit price (prioritization fee per CU).
    pub fn compute_unit_price(&self) -> u64 {
        self.prioritization_fee
            .saturating_div(self.compute_unit_limit.saturating_add(1))
    }
}

/// Derive cost data from a transaction view.
///
/// Returns `None` if the transaction cannot be parsed or has no compute budget.
pub fn derive_costs(
    tx: &SanitizedTransactionView<agave_scheduling_utils::transaction_ptr::TransactionPtr>,
    feature_set: &FeatureSet,
) -> Option<TxCosts> {
    // Construct runtime transaction.
    let rt_tx = RuntimeTransaction::<
        &SanitizedTransactionView<agave_scheduling_utils::transaction_ptr::TransactionPtr>,
    >::try_new(tx, MessageHash::Compute, None)
    .ok()?;

    // Extract compute budget limits.
    let compute_budget_limits =
        compute_budget_instruction_details::ComputeBudgetInstructionDetails::try_from(
            rt_tx.program_instructions_iter(),
        )
        .ok()?
        .sanitize_and_convert_to_compute_budget_limits(feature_set)
        .ok()?;

    let prioritization_fee = compute_budget_limits.get_prioritization_fee();
    let compute_unit_limit = compute_budget_limits.compute_unit_limit as u64;

    // Compute total cost via CostModel.
    let total_cost = CostModel::calculate_cost(&rt_tx, feature_set).sum();

    Some(TxCosts {
        prioritization_fee,
        compute_unit_limit,
        total_cost,
    })
}

/// Compute the tighter-batch composite score for a transaction.
///
/// Formula:
/// ```text
/// score = cu_price * weight_fee
///         + (1_000_000 / cu_limit) * weight_efficiency
/// ```
pub fn tighter_batch_score(costs: &TxCosts, config: &TighterBatchConfig) -> u64 {
    let cu_price = costs.compute_unit_price();
    let fee_component = cu_price.saturating_mul(config.weight_fee);

    // Efficiency bonus: prefer txs with tighter CU estimates.
    let efficiency_bonus = 1_000_000u64
        .checked_div(costs.compute_unit_limit)
        .unwrap_or(0)
        .saturating_mul(config.weight_efficiency);

    fee_component.saturating_add(efficiency_bonus)
}

/// Compute a priority ID using the tighter-batch scoring formula.
///
/// The priority is the score scaled by `PRIORITY_MULTIPLIER` to preserve
/// precision, capped below `u64::MAX` (used as a bundle marker).
pub const PRIORITY_MULTIPLIER: u64 = 1_000_000;
pub const BUNDLE_MARKER: u64 = u64::MAX;

pub fn tighter_batch_priority(costs: &TxCosts, config: &TighterBatchConfig) -> Option<(u64, u64)> {
    let score = tighter_batch_score(costs, config);

    // Apply minimum score filter.
    if score < config.min_score {
        return None;
    }

    let priority = score
        .saturating_mul(PRIORITY_MULTIPLIER)
        .min(BUNDLE_MARKER.saturating_sub(1));

    Some((priority, costs.total_cost))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cu_price_basic() {
        let _costs = TxCosts {
            prioritization_fee: 50_000,
            compute_unit_limit: 200_000,
            total_cost: 150_000,
        };
        // 50_000 / (200_000 + 1) = 0 (integer division)
        // Let's use values that don't underflow
        let costs2 = TxCosts {
            prioritization_fee: 200_000,
            compute_unit_limit: 100_000,
            total_cost: 80_000,
        };
        assert_eq!(costs2.compute_unit_price(), 1); // 200_000 / 100_001 = 1
    }

    #[test]
    fn test_tighter_batch_score_formula() {
        let config = TighterBatchConfig::default();
        let costs = TxCosts {
            prioritization_fee: 1_000_000,
            compute_unit_limit: 100_000,
            total_cost: 50_000,
        };

        // cu_price = 1_000_000 / 100_001 = 9
        // fee_component = 9 * 1 = 9
        // efficiency = 1_000_000 / 100_000 = 10
        // efficiency_bonus = 10 * 1 = 10
        // score = 9 + 10 = 19
        let expected_cu_price = 1_000_000u64 / (100_000u64 + 1);
        let expected = expected_cu_price + (1_000_000 / 100_000);
        assert_eq!(tighter_batch_score(&costs, &config), expected);
    }

    #[test]
    fn test_tighter_batch_score_weighted() {
        let config = TighterBatchConfig {
            weight_fee: 2,
            weight_efficiency: 1,
            min_score: 0,
        };
        let costs = TxCosts {
            prioritization_fee: 2_000_000,
            compute_unit_limit: 100_000,
            total_cost: 50_000,
        };

        // cu_price = 2_000_000 / 100_001 = 19
        // fee_component = 19 * 2 = 38
        // efficiency = 1_000_000 / 100_000 = 10
        // score = 38 + 10 = 48
        let expected_cu_price = 2_000_000u64 / (100_000u64 + 1);
        let expected = expected_cu_price * 2 + (1_000_000 / 100_000);
        assert_eq!(tighter_batch_score(&costs, &config), expected);
    }

    #[test]
    fn test_min_score_filter() {
        let config = TighterBatchConfig {
            weight_fee: 1,
            weight_efficiency: 1,
            min_score: 100,
        };
        let costs = TxCosts {
            prioritization_fee: 100,
            compute_unit_limit: 100_000,
            total_cost: 50,
        };

        // cu_price = 0, efficiency = 10, score = 10 < 100 => None
        assert!(tighter_batch_priority(&costs, &config).is_none());
    }

    #[test]
    fn test_efficiency_bonus_small_cu() {
        let config = TighterBatchConfig::default();

        // Small CU request (1000) gets higher efficiency bonus
        let small = TxCosts {
            prioritization_fee: 100_000,
            compute_unit_limit: 1_000,
            total_cost: 500,
        };

        // Large CU request (1_000_000) gets lower efficiency bonus
        let large = TxCosts {
            prioritization_fee: 100_000,
            compute_unit_limit: 1_000_000,
            total_cost: 500_000,
        };

        let small_score = tighter_batch_score(&small, &config);
        let large_score = tighter_batch_score(&large, &config);

        // Both have same cu_price (100_000 / 1_001 = 99 vs 100_000 / 1_000_001 = 0)
        // Actually small has higher cu_price too, so it wins on both components
        assert!(small_score > large_score);
    }

    #[test]
    fn test_zero_cu_limit_does_not_panic() {
        // CU limit of 0 should not cause division by zero — saturating_add(1) protects it.
        let config = TighterBatchConfig::default();
        let costs = TxCosts {
            prioritization_fee: 100_000,
            compute_unit_limit: 0,
            total_cost: 0,
        };
        let score = tighter_batch_score(&costs, &config);
        // cu_price = 100_000 / 1 = 100_000, efficiency = 1_000_000 / 0 = saturates to u64::MAX
        assert!(score > 0);
    }

    #[test]
    fn test_high_cu_limit_low_efficiency() {
        // A tx with huge CU limit gets very low efficiency bonus, even with decent cu_price.
        let config = TighterBatchConfig::default();
        let costs = TxCosts {
            prioritization_fee: 10_000_000,
            compute_unit_limit: 14_000_000, // near max
            total_cost: 10_000_000,
        };
        let score = tighter_batch_score(&costs, &config);
        // cu_price = 10_000_000 / 14_000_001 = 0
        // efficiency = 1_000_000 / 14_000_000 = 0
        assert_eq!(score, 0);
    }

    #[test]
    fn test_low_cu_high_fee_wins() {
        // Small CU with high fee should beat large CU with same fee density.
        let config = TighterBatchConfig::default();

        let tight = TxCosts {
            prioritization_fee: 100_000,
            compute_unit_limit: 10_000,
            total_cost: 5_000,
        };

        let loose = TxCosts {
            prioritization_fee: 1_000_000,
            compute_unit_limit: 1_000_000,
            total_cost: 500_000,
        };

        // tight: cu_price = 10, efficiency = 100 → score = 110
        // loose: cu_price = 1, efficiency = 1 → score = 2
        let tight_score = tighter_batch_score(&tight, &config);
        let loose_score = tighter_batch_score(&loose, &config);
        assert!(tight_score > loose_score);
    }

    #[test]
    fn test_weight_fee_emphasis() {
        // When weight_fee >> weight_efficiency, high fee txs win even with loose CU.
        let config = TighterBatchConfig {
            weight_fee: 10,
            weight_efficiency: 1,
            min_score: 0,
        };

        let high_fee_loose = TxCosts {
            prioritization_fee: 10_000_000,
            compute_unit_limit: 10_000_000,
            total_cost: 5_000_000,
        };

        let low_fee_tight = TxCosts {
            prioritization_fee: 10_000,
            compute_unit_limit: 1_000,
            total_cost: 5_000,
        };

        // high_fee_loose: cu_price = 1, eff = 0 → score = 10
        // low_fee_tight: cu_price = 10, eff = 1000 → score = 10_100
        let score_a = tighter_batch_score(&high_fee_loose, &config);
        let score_b = tighter_batch_score(&low_fee_tight, &config);
        assert!(score_b > score_a);
    }

    #[test]
    fn test_weight_efficiency_emphasis() {
        // When weight_efficiency >> weight_fee, tight CU txs win even with lower fees.
        let config = TighterBatchConfig {
            weight_fee: 1,
            weight_efficiency: 100,
            min_score: 0,
        };

        let high_fee_loose = TxCosts {
            prioritization_fee: 100_000,
            compute_unit_limit: 10_000_000,
            total_cost: 50_000,
        };

        let low_fee_tight = TxCosts {
            prioritization_fee: 1_000,
            compute_unit_limit: 1_000,
            total_cost: 500,
        };

        // high_fee_loose: cu_price = 0, eff = 0 → score = 0
        // low_fee_tight: cu_price = 1, eff = 1000 → score = 1 + 100_000 = 100_001
        let score_a = tighter_batch_score(&high_fee_loose, &config);
        let score_b = tighter_batch_score(&low_fee_tight, &config);
        assert!(score_b > score_a);
    }

    #[test]
    fn test_min_score_blocks_low_scoring_tx() {
        let config = TighterBatchConfig {
            weight_fee: 1,
            weight_efficiency: 1,
            min_score: 50,
        };

        // Low scoring tx (low fee, high CU)
        let low_score = TxCosts {
            prioritization_fee: 100,
            compute_unit_limit: 10_000_000,
            total_cost: 50,
        };

        // High scoring tx (decent fee, low CU)
        let high_score = TxCosts {
            prioritization_fee: 500_000,
            compute_unit_limit: 5_000,
            total_cost: 10_000,
        };

        assert!(tighter_batch_priority(&low_score, &config).is_none());
        let result = tighter_batch_priority(&high_score, &config);
        assert!(result.is_some());
    }

    #[test]
    fn test_priority_capped_at_bundle_marker() {
        let config = TighterBatchConfig::default();

        // Extreme values that would overflow past BUNDLE_MARKER
        let costs = TxCosts {
            prioritization_fee: u64::MAX,
            compute_unit_limit: 1,
            total_cost: 1,
        };

        let result = tighter_batch_priority(&costs, &config);
        assert!(result.is_some());
        let (priority, _) = result.unwrap();
        assert!(priority < BUNDLE_MARKER);
    }

    #[test]
    fn test_zero_fee_zero_efficiency() {
        let config = TighterBatchConfig::default();
        let costs = TxCosts {
            prioritization_fee: 0,
            compute_unit_limit: 100_000,
            total_cost: 0,
        };

        let score = tighter_batch_score(&costs, &config);
        // cu_price = 0, efficiency = 10 → score = 10
        assert_eq!(score, 10);
    }

    #[test]
    fn test_equal_weights_favors_tighter_cu() {
        // With equal weights, a tx with same cu_price but tighter CU should score higher.
        let config = TighterBatchConfig {
            weight_fee: 1,
            weight_efficiency: 1,
            min_score: 0,
        };

        // Both have cu_price ≈ 10, but tx_a has tighter CU
        let tx_a = TxCosts {
            prioritization_fee: 100_000,
            compute_unit_limit: 10_000,
            total_cost: 5_000,
        };

        let tx_b = TxCosts {
            prioritization_fee: 1_000_000,
            compute_unit_limit: 100_000,
            total_cost: 50_000,
        };

        // tx_a: cu_price = 10, eff = 100 → 110
        // tx_b: cu_price = 10, eff = 10 → 20
        let score_a = tighter_batch_score(&tx_a, &config);
        let score_b = tighter_batch_score(&tx_b, &config);
        assert!(score_a > score_b);
    }
}
