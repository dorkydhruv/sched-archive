pub mod events;
mod shared;

use agave_scheduling_utils::{bridge::RuntimeState, transaction_ptr::TransactionPtr};
use agave_transaction_view::transaction_view::SanitizedTransactionView;
pub use shared::PriorityId;
use solana_compute_budget_instruction::compute_budget_instruction_details;
use solana_cost_model::cost_model::CostModel;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_transaction::sanitized::MessageHash;

pub fn calculate_cost_and_reward(
    runtime: &RuntimeState,
    tx: &SanitizedTransactionView<TransactionPtr>,
) -> Option<(u64, u64)> {
    // Construct runtime transaction.
    let tx = RuntimeTransaction::<&SanitizedTransactionView<TransactionPtr>>::try_new(
        tx,
        MessageHash::Compute,
        None,
    )
    .ok()?;

    // Compute transaction cost.
    let compute_budget_limits =
        compute_budget_instruction_details::ComputeBudgetInstructionDetails::try_from(
            tx.program_instructions_iter(),
        )
        .ok()?
        .sanitize_and_convert_to_compute_budget_limits(&runtime.feature_set)
        .ok()?;
    let cost = CostModel::calculate_cost(&tx, &runtime.feature_set).sum();

    // Compute transaction reward.
    let fee_details = solana_fee::calculate_fee_details(
        &tx,
        runtime.lamports_per_signature,
        compute_budget_limits.get_prioritization_fee(),
        runtime.fee_features,
    );
    let burn = fee_details
        .transaction_fee()
        .checked_mul(runtime.burn_percent)?
        / 100;
    let base_fee = fee_details.transaction_fee() - burn;
    let reward = base_fee.saturating_add(fee_details.prioritization_fee());

    Some((cost, reward))
}
