use crate::rpc::summit::EpochBoundsResponse;

pub(crate) struct CheckpointSchedule {
    pub ready_block: u64,
    pub checkpoint_block: u64,
}

pub(crate) fn summit_checkpoint_schedule(
    bounds: EpochBoundsResponse,
    checkpoint_delay_blocks: u64,
) -> CheckpointSchedule {
    // The next epoch starts one block after last_height. Summit captures the
    // checkpoint state at the penultimate block of the completed epoch.
    CheckpointSchedule {
        ready_block: bounds.last_height.saturating_add(1).saturating_add(checkpoint_delay_blocks),
        checkpoint_block: bounds.last_height.saturating_sub(1),
    }
}

pub(crate) fn fixed_interval_schedule(
    current_epoch: u64,
    epoch_blocks: u64,
    checkpoint_delay_blocks: u64,
) -> CheckpointSchedule {
    let epoch_boundary = current_epoch.saturating_mul(epoch_blocks);

    CheckpointSchedule {
        ready_block: epoch_boundary.saturating_add(checkpoint_delay_blocks),
        checkpoint_block: epoch_boundary.saturating_sub(2),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summit_schedule_uses_historical_epoch_bounds() {
        let bounds = EpochBoundsResponse { first_height: 30_000, last_height: 37_654 };
        let schedule = summit_checkpoint_schedule(bounds, 3);

        assert_eq!(schedule.checkpoint_block, 37_653);
        assert_eq!(schedule.ready_block, 37_658);
    }

    #[test]
    fn fixed_interval_schedule_preserves_reth_only_behavior() {
        let schedule = fixed_interval_schedule(4, 5_000, 3);

        assert_eq!(schedule.checkpoint_block, 19_998);
        assert_eq!(schedule.ready_block, 20_003);
    }
}
