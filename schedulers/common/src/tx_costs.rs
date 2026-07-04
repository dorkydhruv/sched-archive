use {
    agave_feature_set::FeatureSet,
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    solana_compute_budget_instruction::compute_budget_instruction_details,
    solana_cost_model::cost_model::CostModel,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_transaction::sanitized::MessageHash,
};
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
