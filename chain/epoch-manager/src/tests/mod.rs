mod random_epochs;

use super::*;
use crate::reward_calculator::NUM_NS_IN_SECOND;
use crate::test_utils::{
    DEFAULT_TOTAL_SUPPLY, block_info, change_stake, default_reward_calculator, epoch_config,
    epoch_info, epoch_info_with_num_seats, hash_range, record_block,
    record_block_with_final_block_hash, record_block_with_version, record_blocks,
    record_with_block_info, reward, setup_default_epoch_manager, setup_epoch_manager, stake,
};
use itertools::Itertools;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_crypto::{KeyType, PublicKey};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::id::AccountIdRef;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::block::Tip;
use near_primitives::epoch_block_info::BlockInfoV3;
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::hash::hash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::stateless_validation::partial_witness::PartialEncodedStateWitness;
use near_primitives::types::AccountInfo;
use near_primitives::types::ValidatorKickoutReason::{
    NotEnoughBlocks, NotEnoughChunkEndorsements, NotEnoughChunks, ProtocolVersionTooOld,
};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::ShardUId;
use near_store::test_utils::create_test_store;
use num_rational::Ratio;
use std::cmp::Ordering;
use std::vec;

#[test]
fn test_stake_validator() {
    let amount_staked = 1_000_000;
    let validators = vec![("test1".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators.clone(), 1, 1, 2, 90, 60);

    let h = hash_range(4);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

    let expected0 = epoch_info_with_num_seats(
        1,
        vec![("test1".parse().unwrap(), amount_staked)],
        vec![0, 0],
        vec![vec![0, 0]],
        change_stake(vec![("test1".parse().unwrap(), amount_staked)]),
        vec![],
        reward(vec![("near".parse().unwrap(), 0)]),
        0,
        4,
        PROTOCOL_VERSION,
    );
    let compare_epoch_infos = |a: &EpochInfo, b: &EpochInfo| -> bool {
        a.validators_iter().eq(b.validators_iter())
            && a.fishermen_iter().eq(b.fishermen_iter())
            && a.stake_change() == b.stake_change()
            && a.validator_kickout() == b.validator_kickout()
            && a.validator_reward() == b.validator_reward()
    };
    let epoch0 = epoch_manager.get_epoch_id(&h[0]).unwrap();
    assert!(compare_epoch_infos(&epoch_manager.get_epoch_info(&epoch0).unwrap(), &expected0));

    record_block(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![stake("test2".parse().unwrap(), amount_staked)],
    );
    let epoch1 = epoch_manager.get_epoch_id(&h[1]).unwrap();
    assert!(compare_epoch_infos(&epoch_manager.get_epoch_info(&epoch1).unwrap(), &expected0));
    assert_eq!(epoch_manager.get_epoch_id(&h[2]), Err(EpochError::MissingBlock(h[2])));

    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    // test2 staked in epoch 1 and therefore should be included in epoch 3.
    let epoch2 = epoch_manager.get_epoch_id(&h[2]).unwrap();
    assert!(compare_epoch_infos(&epoch_manager.get_epoch_info(&epoch2).unwrap(), &expected0));

    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);

    let expected3 = epoch_info_with_num_seats(
        2,
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)],
        vec![0, 1],
        vec![vec![0, 1]],
        change_stake(vec![
            ("test1".parse().unwrap(), amount_staked),
            ("test2".parse().unwrap(), amount_staked),
        ]),
        vec![],
        // only the validator who produced the block in this epoch gets the reward since epoch length is 1
        reward(vec![("test1".parse().unwrap(), 0), ("near".parse().unwrap(), 0)]),
        0,
        4,
        PROTOCOL_VERSION,
    );
    // no validator change in the last epoch
    let epoch3 = epoch_manager.get_epoch_id(&h[3]).unwrap();
    assert!(compare_epoch_infos(&epoch_manager.get_epoch_info(&epoch3).unwrap(), &expected3));

    // Start another epoch manager from the same store to check that it saved the state.
    let epoch_manager2 = EpochManager::new(
        epoch_manager.store.clone(),
        epoch_manager.config.clone(),
        epoch_manager.reward_calculator,
        validators
            .iter()
            .map(|(account_id, balance)| stake(account_id.clone(), *balance))
            .collect(),
    )
    .unwrap();
    assert!(compare_epoch_infos(&epoch_manager2.get_epoch_info(&epoch3).unwrap(), &expected3));
}

#[test]
fn test_validator_change_of_stake() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager =
        setup_epoch_manager(validators, 2, 1, 2, 90, 60, 0, default_reward_calculator());

    let h = hash_range(4);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 10)]);
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    // New epoch starts here.
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![("test1".parse().unwrap(), 0), ("test2".parse().unwrap(), amount_staked)],
    );
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
        ],
    );
    matches!(
        epoch_info.validator_kickout().get(AccountIdRef::new_or_panic("test1")),
        Some(ValidatorKickoutReason::NotEnoughStake { stake: 10, .. })
    );
}

/// Test handling forks across the epoch finalization.
/// Fork with where one BP produces blocks in one chain and 2 BPs are in another chain.
///     |   | /--1---4------|--7---10------|---13---
///   x-|-0-|-
///     |   | \--2---3---5--|---6---8---9--|----11---12--
/// In upper fork, only test2 left + new validator test4.
/// In lower fork, test1 and test3 are left.
#[test]
fn test_fork_finalization() {
    let amount_staked = 1_000_000;
    let validators = vec![
        ("test1".parse().unwrap(), amount_staked),
        ("test2".parse().unwrap(), amount_staked),
        ("test3".parse().unwrap(), amount_staked),
    ];
    let epoch_length = 20;
    let epoch_manager =
        setup_default_epoch_manager(validators.clone(), epoch_length, 1, 3, 90, 60).into_handle();

    let h = hash_range((5 * epoch_length - 1) as usize);
    // Have an alternate set of hashes to use on the other branch to avoid collisions.
    let h2: Vec<CryptoHash> = h.iter().map(|x| hash(x.as_ref())).collect();

    record_block(&mut epoch_manager.write(), CryptoHash::default(), h[0], 0, vec![]);

    let build_branch = |epoch_manager: EpochManagerHandle,
                        base_block: CryptoHash,
                        hashes: &[CryptoHash],
                        validator_accounts: &[&str]|
     -> Vec<CryptoHash> {
        let mut prev_block = base_block;
        let mut branch_blocks = Vec::new();
        for (i, curr_block) in hashes.iter().enumerate().skip(2) {
            let height = i as u64;
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
            let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap().clone();
            let block_producer_id = epoch_info.sample_block_producer(height);
            let block_producer = epoch_info.get_validator(block_producer_id);
            let account_id = block_producer.account_id();
            if validator_accounts.iter().any(|v| *v == account_id) {
                record_block(&mut epoch_manager.write(), prev_block, *curr_block, height, vec![]);
                prev_block = *curr_block;
                branch_blocks.push(*curr_block);
            }
        }
        branch_blocks
    };

    // build test2/test4 fork
    record_block(
        &mut epoch_manager.write(),
        h[0],
        h[1],
        1,
        vec![stake("test4".parse().unwrap(), amount_staked)],
    );
    let blocks_test2 = build_branch(epoch_manager.clone(), h[1], &h, &["test2", "test4"]);

    // build test1/test3 fork
    let blocks_test1 = build_branch(epoch_manager.clone(), h[0], &h2, &["test1", "test3"]);

    let epoch1 = epoch_manager.get_epoch_id(&h[1]).unwrap();
    let mut bps = epoch_manager
        .read()
        .get_all_block_producers_ordered(&epoch1)
        .unwrap()
        .iter()
        .map(|x| x.account_id().clone())
        .collect::<Vec<_>>();
    bps.sort_unstable();
    let expected_bps: Vec<AccountId> =
        vec!["test1".parse().unwrap(), "test2".parse().unwrap(), "test3".parse().unwrap()];
    assert_eq!(bps, expected_bps);
    let last_block = blocks_test2.last().unwrap();
    let epoch2_1 = epoch_manager.get_epoch_id(last_block).unwrap();
    assert_eq!(
        epoch_manager
            .read()
            .get_all_block_producers_ordered(&epoch2_1)
            .unwrap()
            .iter()
            .map(|x| x.account_id().clone())
            .collect::<Vec<_>>(),
        vec!["test2".parse::<AccountId>().unwrap(), "test4".parse().unwrap()]
    );

    let last_block = blocks_test1.last().unwrap();
    let epoch2_2 = epoch_manager.get_epoch_id(last_block).unwrap();
    assert_eq!(
        epoch_manager
            .read()
            .get_all_block_producers_ordered(&epoch2_2)
            .unwrap()
            .iter()
            .map(|x| x.account_id().clone())
            .collect::<Vec<_>>(),
        vec!["test1".parse::<AccountId>().unwrap(), "test3".parse().unwrap(),]
    );

    // Check that if we have a different epoch manager and apply only second branch we get the same results.
    let epoch_manager2 =
        setup_default_epoch_manager(validators, epoch_length, 1, 3, 90, 60).into_handle();
    record_block(&mut epoch_manager2.write(), CryptoHash::default(), h[0], 0, vec![]);
    build_branch(epoch_manager2.clone(), h[0], &h2, &["test1", "test3"]);
    assert_eq!(epoch_manager.get_epoch_info(&epoch2_2), epoch_manager2.get_epoch_info(&epoch2_2));
}

/// In the case where there is only one validator and the
/// number of blocks produced by the validator is under the
/// threshold for some given epoch, the validator should not
/// be kicked out
#[test]
fn test_one_validator_kickout() {
    let amount_staked = 1_000;
    let mut epoch_manager = setup_default_epoch_manager(
        vec![("test1".parse().unwrap(), amount_staked)],
        2,
        1,
        1,
        90,
        60,
    );

    let h = hash_range(6);
    // this validator only produces one block every epoch whereas they should have produced 2. However, since
    // this is the only validator left, we still keep them as validator.
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[2], 2, vec![]);
    record_block(&mut epoch_manager, h[2], h[4], 4, vec![]);
    record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[5]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test1", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_kickout(&epoch_info, &[]);
    check_stake_change(&epoch_info, vec![("test1".parse().unwrap(), amount_staked)]);
}

/// When computing validator kickout, we should not kickout validators such that the union
/// of kickout for this epoch and last epoch equals the entire validator set.
#[test]
fn test_validator_kickout() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let epoch_length = 10;
    let epoch_manager =
        setup_default_epoch_manager(validators, epoch_length, 1, 2, 90, 60).into_handle();
    let h = hash_range((3 * epoch_length) as usize);

    record_block(&mut epoch_manager.write(), CryptoHash::default(), h[0], 0, vec![]);
    let mut prev_block = h[0];
    let mut test2_expected_blocks = 0;
    let init_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
    for (i, curr_block) in h.iter().enumerate().skip(1) {
        let height = i as u64;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
        let block_producer = epoch_manager.get_block_producer_info(&epoch_id, height).unwrap();
        if block_producer.account_id() == "test2" && epoch_id == init_epoch_id {
            // test2 skips its blocks in the first epoch
            test2_expected_blocks += 1;
        } else if block_producer.account_id() == "test1" && epoch_id != init_epoch_id {
            // test1 skips its blocks in subsequent epochs
            ()
        } else {
            record_block(&mut epoch_manager.write(), prev_block, *curr_block, height, vec![]);
            prev_block = *curr_block;
        }
    }
    let epoch_infos: Vec<_> =
        h.iter().filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok()).collect();
    check_kickout(
        &epoch_infos[1],
        &[("test2", NotEnoughBlocks { produced: 0, expected: test2_expected_blocks })],
    );
    let epoch_info = &epoch_infos[2];
    check_validators(epoch_info, &[("test1", amount_staked)]);
    check_fishermen(epoch_info, &[]);
    check_stake_change(epoch_info, vec![("test1".parse().unwrap(), amount_staked)]);
    check_kickout(epoch_info, &[]);
    check_reward(
        epoch_info,
        vec![
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
            ("test1".parse().unwrap(), 0),
        ],
    );
}

#[test]
fn test_validator_unstake() {
    let store = create_test_store();
    let config = epoch_config(2, 1, 2, 100, 90, 60, 0);
    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked),
    ];
    let mut epoch_manager =
        EpochManager::new(store, config, default_reward_calculator(), validators).unwrap();
    let h = hash_range(8);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 0)]);
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);

    let epoch_id = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![("test1".parse().unwrap(), 0), ("test2".parse().unwrap(), amount_staked)],
    );
    check_kickout(&epoch_info, &[("test1", ValidatorKickoutReason::Unstaked)]);
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
        ],
    );

    record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
    record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[5]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(&epoch_info, vec![("test2".parse().unwrap(), amount_staked)]);
    check_kickout(&epoch_info, &[]);
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
        ],
    );

    record_block(&mut epoch_manager, h[5], h[6], 6, vec![]);
    record_block(&mut epoch_manager, h[6], h[7], 7, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[7]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(&epoch_info, vec![("test2".parse().unwrap(), amount_staked)]);
    check_kickout(&epoch_info, &[]);
    check_reward(&epoch_info, vec![("test2".parse().unwrap(), 0), ("near".parse().unwrap(), 0)]);
}

/// If all current validator try to unstake, we disallow that.
#[test]
fn test_all_validators_unstake() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 90, 60);
    let h = hash_range(5);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    // all validators are trying to unstake.
    record_block(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![
            stake("test1".parse().unwrap(), 0),
            stake("test2".parse().unwrap(), 0),
            stake("test3".parse().unwrap(), 0),
        ],
    );
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    let next_epoch = epoch_manager.get_next_epoch_id(&h[2]).unwrap();
    assert_eq!(
        epoch_manager.get_epoch_info(&next_epoch).unwrap().validators_iter().collect::<Vec<_>>(),
        vec![
            stake("test1".parse().unwrap(), stake_amount),
            stake("test2".parse().unwrap(), stake_amount),
            stake("test3".parse().unwrap(), stake_amount)
        ],
    );
}

#[test]
fn test_validator_reward_one_validator() {
    let stake_amount = 1_000_000;
    let test1_stake_amount = 110;
    let validators = vec![
        ("test1".parse().unwrap(), test1_stake_amount),
        ("test2".parse().unwrap(), stake_amount),
    ];
    let epoch_length = 2;
    let total_supply = validators.iter().map(|(_, stake)| stake).sum();
    let reward_calculator = RewardCalculator {
        max_inflation_rate: Ratio::new(5, 100),
        num_blocks_per_year: 50,
        epoch_length,
        protocol_reward_rate: Ratio::new(1, 10),
        protocol_treasury_account: "near".parse().unwrap(),
        num_seconds_per_year: 50,
        genesis_protocol_version: PROTOCOL_VERSION,
    };
    let mut epoch_manager =
        setup_epoch_manager(validators, epoch_length, 1, 1, 90, 60, 0, reward_calculator.clone());
    let rng_seed = [0; 32];
    let h = hash_range(5);

    epoch_manager
        .record_block_info(
            block_info(
                h[0],
                0,
                0,
                Default::default(),
                Default::default(),
                h[0],
                vec![true],
                total_supply,
            ),
            rng_seed,
        )
        .unwrap();
    epoch_manager
        .record_block_info(
            block_info(h[1], 1, 1, h[0], h[0], h[1], vec![true], total_supply),
            rng_seed,
        )
        .unwrap();
    epoch_manager
        .record_block_info(
            block_info(h[2], 2, 2, h[1], h[1], h[1], vec![true], total_supply),
            rng_seed,
        )
        .unwrap();
    let mut validator_online_ratio = HashMap::new();
    validator_online_ratio.insert(
        "test2".parse().unwrap(),
        BlockChunkValidatorStats {
            block_stats: ValidatorStats { produced: 1, expected: 1 },
            chunk_stats: ChunkStats::new_with_production(1, 1),
        },
    );
    let mut validator_stakes = HashMap::new();
    validator_stakes.insert("test2".parse().unwrap(), stake_amount);
    let (validator_reward, inflation) = reward_calculator.calculate_reward(
        validator_online_ratio,
        &validator_stakes,
        total_supply,
        PROTOCOL_VERSION,
        epoch_length * NUM_NS_IN_SECOND,
        ValidatorOnlineThresholds {
            online_min_threshold: Ratio::new(90, 100),
            online_max_threshold: Ratio::new(99, 100),
            endorsement_cutoff_threshold: None,
        },
    );
    let test2_reward = *validator_reward.get(AccountIdRef::new_or_panic("test2")).unwrap();
    let protocol_reward = *validator_reward.get(AccountIdRef::new_or_panic("near")).unwrap();

    let epoch_info = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    check_validators(&epoch_info, &[("test2", stake_amount + test2_reward)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(&epoch_info, vec![("test2".parse().unwrap(), stake_amount + test2_reward)]);
    check_kickout(&epoch_info, &[]);
    check_reward(
        &epoch_info,
        vec![("test2".parse().unwrap(), test2_reward), ("near".parse().unwrap(), protocol_reward)],
    );
    assert_eq!(epoch_info.minted_amount(), inflation);
}

#[test]
fn test_validator_reward_weight_by_stake() {
    let stake_amount1 = 1_000_000;
    let stake_amount2 = 500_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount1), ("test2".parse().unwrap(), stake_amount2)];
    let epoch_length = 2;
    let total_supply = (stake_amount1 + stake_amount2) * validators.len() as u128;
    let reward_calculator = RewardCalculator {
        max_inflation_rate: Ratio::new(5, 100),
        num_blocks_per_year: 50,
        epoch_length,
        protocol_reward_rate: Ratio::new(1, 10),
        protocol_treasury_account: "near".parse().unwrap(),
        num_seconds_per_year: 50,
        genesis_protocol_version: PROTOCOL_VERSION,
    };
    let mut epoch_manager =
        setup_epoch_manager(validators, epoch_length, 1, 2, 90, 60, 0, reward_calculator.clone());
    let h = hash_range(5);
    record_with_block_info(
        &mut epoch_manager,
        block_info(
            h[0],
            0,
            0,
            Default::default(),
            Default::default(),
            h[0],
            vec![true],
            total_supply,
        ),
    );
    record_with_block_info(
        &mut epoch_manager,
        block_info(h[1], 1, 1, h[0], h[0], h[1], vec![true], total_supply),
    );
    record_with_block_info(
        &mut epoch_manager,
        block_info(h[2], 2, 2, h[1], h[1], h[1], vec![true], total_supply),
    );
    let mut validator_online_ratio = HashMap::new();
    validator_online_ratio.insert(
        "test1".parse().unwrap(),
        BlockChunkValidatorStats {
            block_stats: ValidatorStats { produced: 1, expected: 1 },
            chunk_stats: ChunkStats::new_with_production(1, 1),
        },
    );
    validator_online_ratio.insert(
        "test2".parse().unwrap(),
        BlockChunkValidatorStats {
            block_stats: ValidatorStats { produced: 1, expected: 1 },
            chunk_stats: ChunkStats::new_with_production(1, 1),
        },
    );
    let mut validators_stakes = HashMap::new();
    validators_stakes.insert("test1".parse().unwrap(), stake_amount1);
    validators_stakes.insert("test2".parse().unwrap(), stake_amount2);
    let (validator_reward, inflation) = reward_calculator.calculate_reward(
        validator_online_ratio,
        &validators_stakes,
        total_supply,
        PROTOCOL_VERSION,
        epoch_length * NUM_NS_IN_SECOND,
        ValidatorOnlineThresholds {
            online_min_threshold: Ratio::new(90, 100),
            online_max_threshold: Ratio::new(99, 100),
            endorsement_cutoff_threshold: None,
        },
    );
    let test1_reward = *validator_reward.get(AccountIdRef::new_or_panic("test1")).unwrap();
    let test2_reward = *validator_reward.get(AccountIdRef::new_or_panic("test2")).unwrap();
    assert_eq!(test1_reward, test2_reward * 2);
    let protocol_reward = *validator_reward.get(AccountIdRef::new_or_panic("near")).unwrap();

    let epoch_info = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    check_validators(
        &epoch_info,
        &[("test1", stake_amount1 + test1_reward), ("test2", stake_amount2 + test2_reward)],
    );
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), stake_amount1 + test1_reward),
            ("test2".parse().unwrap(), stake_amount2 + test2_reward),
        ],
    );
    check_kickout(&epoch_info, &[]);
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), test1_reward),
            ("test2".parse().unwrap(), test2_reward),
            ("near".parse().unwrap(), protocol_reward),
        ],
    );
    assert_eq!(epoch_info.minted_amount(), inflation);
}

#[test]
fn test_reward_multiple_shards() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 10;
    let total_supply = stake_amount * validators.len() as u128;
    let reward_calculator = RewardCalculator {
        max_inflation_rate: Ratio::new(5, 100),
        num_blocks_per_year: 1_000_000,
        epoch_length,
        protocol_reward_rate: Ratio::new(1, 10),
        protocol_treasury_account: "near".parse().unwrap(),
        num_seconds_per_year: 1_000_000,
        genesis_protocol_version: PROTOCOL_VERSION,
    };
    let num_shards = 2;
    let epoch_manager = setup_epoch_manager(
        validators,
        epoch_length,
        num_shards,
        2,
        90,
        60,
        0,
        reward_calculator.clone(),
    )
    .into_handle();
    let h = hash_range((2 * epoch_length + 1) as usize);
    record_with_block_info(
        &mut epoch_manager.write(),
        block_info(
            h[0],
            0,
            0,
            Default::default(),
            Default::default(),
            h[0],
            vec![true],
            total_supply,
        ),
    );
    let mut expected_chunks = 0;
    let init_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&h[0]).unwrap();
    for height in 1..(2 * epoch_length) {
        let i = height as usize;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&h[i - 1]).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        // test1 skips its chunks in the first epoch
        let chunk_mask = shard_layout
            .shard_ids()
            .map(|shard_id| {
                let chunk_production_key =
                    ChunkProductionKey { epoch_id, height_created: height, shard_id };
                let expected_chunk_producer =
                    epoch_manager.get_chunk_producer_info(&chunk_production_key).unwrap();
                if expected_chunk_producer.account_id() == "test1" && epoch_id == init_epoch_id {
                    expected_chunks += 1;
                    false
                } else {
                    true
                }
            })
            .collect();
        record_with_block_info(
            &mut epoch_manager.write(),
            block_info(h[i], height, height, h[i - 1], h[i - 1], h[i], chunk_mask, total_supply),
        );
    }
    let mut validator_online_ratio = HashMap::new();
    validator_online_ratio.insert(
        "test2".parse().unwrap(),
        BlockChunkValidatorStats {
            block_stats: ValidatorStats { produced: 1, expected: 1 },
            chunk_stats: ChunkStats::new_with_production(1, 1),
        },
    );
    let mut validators_stakes = HashMap::new();
    validators_stakes.insert("test1".parse().unwrap(), stake_amount);
    validators_stakes.insert("test2".parse().unwrap(), stake_amount);
    let (validator_reward, inflation) = reward_calculator.calculate_reward(
        validator_online_ratio,
        &validators_stakes,
        total_supply,
        PROTOCOL_VERSION,
        epoch_length * NUM_NS_IN_SECOND,
        ValidatorOnlineThresholds {
            online_min_threshold: Ratio::new(90, 100),
            online_max_threshold: Ratio::new(99, 100),
            endorsement_cutoff_threshold: None,
        },
    );
    let test2_reward = *validator_reward.get(AccountIdRef::new_or_panic("test2")).unwrap();
    let protocol_reward = *validator_reward.get(AccountIdRef::new_or_panic("near")).unwrap();
    let epoch_infos: Vec<_> =
        h.iter().filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok()).collect();
    let epoch_info = &epoch_infos[1];
    check_validators(epoch_info, &[("test2", stake_amount + test2_reward)]);
    check_fishermen(epoch_info, &[]);
    check_stake_change(
        epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), stake_amount + test2_reward),
        ],
    );
    check_kickout(
        epoch_info,
        &[("test1", NotEnoughChunks { produced: 0, expected: expected_chunks })],
    );
    check_reward(
        epoch_info,
        vec![("test2".parse().unwrap(), test2_reward), ("near".parse().unwrap(), protocol_reward)],
    );
    assert_eq!(epoch_info.minted_amount(), inflation);
}

#[test]
fn test_unstake_and_then_change_stake() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 90, 60);
    let h = hash_range(8);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 0)]);
    record_block(
        &mut epoch_manager,
        h[1],
        h[2],
        2,
        vec![stake("test1".parse().unwrap(), amount_staked)],
    );
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
    assert_eq!(epoch_id, EpochId(h[2]));
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test1", amount_staked), ("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)],
    );
    check_kickout(&epoch_info, &[]);
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
        ],
    );
}

/// When a block producer fails to produce a block, check that other chunk
/// producers and validators who produce chunks for that block are not kicked
/// out because of it.
#[test]
fn test_expected_chunks() {
    let stake_amount = 1_000_000;
    let validators: Vec<(AccountId, u128)> = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
        ("test4".parse().unwrap(), stake_amount),
    ];
    let epoch_length = 20;
    let num_shards = 3;
    let total_supply = stake_amount * validators.len() as u128;

    let epoch_config = epoch_config(epoch_length, num_shards, 3, 3, 90, 60, 60);
    let epoch_manager = EpochManager::new(
        create_test_store(),
        epoch_config,
        default_reward_calculator(),
        validators
            .iter()
            .map(|(account_id, balance)| stake(account_id.clone(), *balance))
            .collect(),
    )
    .unwrap()
    .into_handle();
    let rng_seed = [0; 32];
    let hashes = hash_range((2 * epoch_length) as usize);
    record_block(&mut epoch_manager.write(), Default::default(), hashes[0], 0, vec![]);
    let mut expected = 0;
    let mut prev_block = hashes[0];
    let initial_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
    for (i, curr_block) in hashes.iter().enumerate().skip(1) {
        let height = i as u64;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap().clone();
        let block_producer = epoch_info.sample_block_producer(height);
        // test1 does not produce blocks during first epoch
        if block_producer == 0 && epoch_id == initial_epoch_id {
            expected += 1;
            continue;
        }

        epoch_manager
            .write()
            .record_block_info(
                block_info(
                    *curr_block,
                    height,
                    height,
                    prev_block,
                    prev_block,
                    epoch_id.0,
                    vec![true, true, true],
                    total_supply,
                ),
                rng_seed,
            )
            .unwrap()
            .commit()
            .unwrap();
        prev_block = *curr_block;

        if epoch_id != initial_epoch_id {
            break;
        }
    }
    let epoch_info = hashes
        .iter()
        .filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok())
        .next_back()
        .unwrap();
    assert_eq!(
        epoch_info.validator_kickout(),
        &[("test1".parse::<AccountId>().unwrap(), NotEnoughBlocks { produced: 0, expected })]
            .into_iter()
            .collect::<HashMap<_, _>>()
    );
}

#[test]
fn test_expected_chunks_prev_block_not_produced() {
    let stake_amount = 1_000_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let epoch_length = 50;
    let total_supply = stake_amount * validators.len() as u128;
    let epoch_manager =
        setup_epoch_manager(validators, epoch_length, 1, 3, 90, 90, 0, default_reward_calculator())
            .into_handle();
    let rng_seed = [0; 32];
    let hashes = hash_range((2 * epoch_length) as usize);
    record_block(&mut epoch_manager.write(), Default::default(), hashes[0], 0, vec![]);
    let mut expected = 0;
    let mut prev_block = hashes[0];
    let initial_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
    for (i, curr_block) in hashes.iter().enumerate().skip(1) {
        let height = i as u64;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap().clone();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let block_producer = epoch_info.sample_block_producer(height);
        let prev_block_info = epoch_manager.get_block_info(&prev_block).unwrap();
        let prev_height = prev_block_info.height();
        let expected_chunk_producer = epoch_info
            .sample_chunk_producer(&shard_layout, ShardId::new(0), prev_height + 1)
            .unwrap();
        // test1 does not produce blocks during first epoch
        if block_producer == 0 && epoch_id == initial_epoch_id {
            expected += 1;
        } else {
            // test1 also misses all their chunks
            let should_produce_chunk = expected_chunk_producer != 0;
            epoch_manager
                .write()
                .record_block_info(
                    block_info(
                        *curr_block,
                        height,
                        height,
                        prev_block,
                        prev_block,
                        epoch_id.0,
                        vec![should_produce_chunk],
                        total_supply,
                    ),
                    rng_seed,
                )
                .unwrap()
                .commit()
                .unwrap();
            prev_block = *curr_block;
        }
        if epoch_id != initial_epoch_id {
            break;
        }
    }
    let epoch_info = hashes
        .iter()
        .filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok())
        .next_back()
        .unwrap();
    assert_eq!(
        epoch_info.validator_kickout(),
        &[("test1".parse().unwrap(), NotEnoughBlocks { produced: 0, expected })]
            .into_iter()
            .collect::<HashMap<_, _>>()
    );
}

fn update_tracker(
    epoch_info: &EpochInfo,
    heights: std::ops::Range<BlockHeight>,
    produced_heights: &[BlockHeight],
    tracker: &mut HashMap<ValidatorId, ValidatorStats>,
) {
    for height in heights {
        let block_producer = epoch_info.sample_block_producer(height);
        let entry =
            tracker.entry(block_producer).or_insert(ValidatorStats { produced: 0, expected: 0 });
        if produced_heights.contains(&height) {
            entry.produced += 1;
        }
        entry.expected += 1;
    }
}

#[test]
fn test_rewards_with_kickouts() {
    let stake_amount = 1_000_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let epoch_length = 10;
    let reward_calculator = RewardCalculator {
        max_inflation_rate: Ratio::new(5, 100),
        num_blocks_per_year: 1,
        epoch_length,
        protocol_reward_rate: Ratio::new(1, 10),
        protocol_treasury_account: "near".parse().unwrap(),
        num_seconds_per_year: NUM_SECONDS_IN_A_YEAR,
        genesis_protocol_version: PROTOCOL_VERSION,
    };
    let em = setup_epoch_manager(validators, epoch_length, 1, 3, 10, 10, 0, reward_calculator)
        .into_handle();

    let mut height: BlockHeight = 0;
    let genesis_hash = hash(height.to_le_bytes().as_ref());
    record_block(&mut em.write(), Default::default(), genesis_hash, height, vec![]);

    height += 1;
    let first_hash = hash(height.to_le_bytes().as_ref());

    // unstake test3 in the first block so we can see it in the kickouts later
    record_block(
        &mut em.write(),
        genesis_hash,
        first_hash,
        height,
        vec![stake("test3".parse().unwrap(), 0)],
    );

    let mut prev_hash = first_hash;
    let mut epoch_ids = Vec::new();

    loop {
        height += 1;
        let block_hash = hash(height.to_le_bytes().as_ref());

        let epoch_id = em.get_epoch_id_from_prev_block(&prev_hash).unwrap();
        let epoch_info = em.get_epoch_info(&epoch_id).unwrap().clone();
        let validator_id = epoch_info.sample_block_producer(height);
        let block_producer = epoch_info.validator_account_id(validator_id);

        // don't produce blocks for test2 so we can see it in the kickouts
        if block_producer.as_str() != "test2" {
            record_block(&mut em.write(), prev_hash, block_hash, height, vec![]);
            prev_hash = block_hash;
        }

        // save new epoch IDs as they come
        if epoch_id != EpochId(CryptoHash::default()) {
            if (epoch_ids.len() as BlockHeight) < epoch_info.epoch_height() {
                // when there are 4 epoch IDs saved, 1 through 4 will be completed, but we only care about
                // the prev epoch kickouts and rewards for 2 through 4 in the checks below
                if epoch_ids.len() >= 4 {
                    break;
                }
                assert!((epoch_ids.len() + 1) as u64 == epoch_info.epoch_height());
                epoch_ids.push(epoch_id);
            }
        }
    }

    let wanted_rewards = HashMap::from([
        (
            2,
            // test3 should still be rewarded even though it is in the kickouts for unstaking
            HashMap::from([
                ("near".parse().unwrap(), 1585),
                ("test1".parse().unwrap(), 4756),
                ("test3".parse().unwrap(), 4756),
            ]),
        ),
        (
            3,
            HashMap::from([
                ("near".parse().unwrap(), 1585),
                ("test1".parse().unwrap(), 4756),
                ("test3".parse().unwrap(), 4756),
            ]),
        ),
        (4, HashMap::from([("near".parse().unwrap(), 1585), ("test1".parse().unwrap(), 14269)])),
    ]);
    let wanted_kickouts = HashMap::from([
        (
            2,
            HashMap::from([
                (
                    "test2".parse().unwrap(),
                    ValidatorKickoutReason::NotEnoughBlocks { produced: 0, expected: 3 },
                ),
                ("test3".parse().unwrap(), ValidatorKickoutReason::Unstaked),
            ]),
        ),
        (
            3,
            HashMap::from([(
                "test2".parse().unwrap(),
                ValidatorKickoutReason::NotEnoughBlocks { produced: 0, expected: 1 },
            )]),
        ),
        (4, HashMap::new()),
    ]);
    for epoch_height in 2..=4 {
        let epoch_id = &epoch_ids[epoch_height - 1];
        let epoch_info = em.get_epoch_info(epoch_id).unwrap();
        assert!(epoch_info.epoch_height() == epoch_height as u64);

        assert_eq!(epoch_info.validator_reward(), wanted_rewards.get(&epoch_height).unwrap());
        assert_eq!(epoch_info.validator_kickout(), wanted_kickouts.get(&epoch_height).unwrap());
    }
}

#[test]
fn test_epoch_info_aggregator() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 5;
    let mut em =
        setup_epoch_manager(validators, epoch_length, 1, 2, 10, 10, 0, default_reward_calculator());
    let h = hash_range(6);
    record_block(&mut em, Default::default(), h[0], 0, vec![]);
    record_block_with_final_block_hash(&mut em, h[0], h[1], h[0], 1, vec![]);
    record_block_with_final_block_hash(&mut em, h[1], h[3], h[0], 3, vec![]);
    assert_eq!(h[0], em.epoch_info_aggregator.last_block_hash);
    let epoch_id = em.get_epoch_id(&h[3]).unwrap();
    let epoch_info = em.get_epoch_info(&epoch_id).unwrap();

    let mut tracker = HashMap::new();
    update_tracker(&epoch_info, 1..4, &[1, 3], &mut tracker);

    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[3]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    // get_epoch_info_aggregator_upto_last does not change
    // epoch_info_aggregator
    assert_eq!(h[0], em.epoch_info_aggregator.last_block_hash);

    record_block_with_final_block_hash(&mut em, h[3], h[5], h[1], 5, vec![]);
    assert_eq!(h[1], em.epoch_info_aggregator.last_block_hash);

    update_tracker(&epoch_info, 4..6, &[5], &mut tracker);

    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[5]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    assert_eq!(h[1], em.epoch_info_aggregator.last_block_hash);
}

/// If the node stops and restarts, the aggregator should be able to recover
#[test]
fn test_epoch_info_aggregator_data_loss() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 5;
    let mut em =
        setup_epoch_manager(validators, epoch_length, 1, 2, 10, 10, 0, default_reward_calculator());
    let h = hash_range(6);
    record_block(&mut em, Default::default(), h[0], 0, vec![]);
    record_block(&mut em, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), stake_amount - 10)]);
    record_block(&mut em, h[1], h[3], 3, vec![stake("test2".parse().unwrap(), stake_amount + 10)]);
    assert_eq!(h[1], em.epoch_info_aggregator.last_block_hash);
    em.epoch_info_aggregator = EpochInfoAggregator::default();
    record_block(&mut em, h[3], h[5], 5, vec![stake("test1".parse().unwrap(), stake_amount - 1)]);
    assert_eq!(h[3], em.epoch_info_aggregator.last_block_hash);
    let epoch_id = em.get_epoch_id(&h[5]).unwrap();
    let epoch_info = em.get_epoch_info(&epoch_id).unwrap();
    let mut tracker = HashMap::new();
    update_tracker(&epoch_info, 1..6, &[1, 3, 5], &mut tracker);
    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[5]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    assert_eq!(
        aggregator.all_proposals,
        vec![
            stake("test1".parse().unwrap(), stake_amount - 1),
            stake("test2".parse().unwrap(), stake_amount + 10)
        ]
        .into_iter()
        .map(|p| (p.account_id().clone(), p))
        .collect::<BTreeMap<_, _>>()
    );
}

/// Aggregator should still work even if there is a reorg past the last final block.
#[test]
fn test_epoch_info_aggregator_reorg_past_final_block() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 6;
    let mut em =
        setup_epoch_manager(validators, epoch_length, 1, 2, 10, 10, 0, default_reward_calculator());
    let h = hash_range(6);
    record_block(&mut em, Default::default(), h[0], 0, vec![]);
    record_block_with_final_block_hash(&mut em, h[0], h[1], h[0], 1, vec![]);
    record_block_with_final_block_hash(&mut em, h[1], h[2], h[0], 2, vec![]);
    record_block_with_final_block_hash(
        &mut em,
        h[2],
        h[3],
        h[1],
        3,
        vec![stake("test1".parse().unwrap(), stake_amount - 1)],
    );
    record_block_with_final_block_hash(&mut em, h[3], h[4], h[3], 4, vec![]);
    record_block_with_final_block_hash(&mut em, h[2], h[5], h[1], 5, vec![]);
    let epoch_id = em.get_epoch_id(&h[5]).unwrap();
    let epoch_info = em.get_epoch_info(&epoch_id).unwrap();
    let mut tracker = HashMap::new();
    update_tracker(&epoch_info, 1..6, &[1, 2, 5], &mut tracker);
    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[5]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    assert!(aggregator.all_proposals.is_empty());
}

#[test]
fn test_epoch_info_aggregator_reorg_beginning_of_epoch() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 4;
    let mut em =
        setup_epoch_manager(validators, epoch_length, 1, 2, 10, 10, 0, default_reward_calculator());
    let h = hash_range(10);
    record_block(&mut em, Default::default(), h[0], 0, vec![]);
    for i in 1..5 {
        record_block(&mut em, h[i - 1], h[i], i as u64, vec![]);
    }
    record_block(&mut em, h[4], h[5], 5, vec![stake("test1".parse().unwrap(), stake_amount - 1)]);
    record_block_with_final_block_hash(
        &mut em,
        h[5],
        h[6],
        h[4],
        6,
        vec![stake("test2".parse().unwrap(), stake_amount - 100)],
    );
    // reorg
    record_block(&mut em, h[4], h[7], 7, vec![]);
    let epoch_id = em.get_epoch_id(&h[7]).unwrap();
    let epoch_info = em.get_epoch_info(&epoch_id).unwrap();
    let mut tracker = HashMap::new();
    update_tracker(&epoch_info, 5..8, &[7], &mut tracker);
    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[7]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    assert!(aggregator.all_proposals.is_empty());
}

fn count_missing_blocks(
    epoch_manager: &dyn EpochManagerAdapter,
    epoch_id: &EpochId,
    height_range: std::ops::Range<u64>,
    produced_heights: &[u64],
    validator: &str,
) -> ValidatorStats {
    let mut result = ValidatorStats { produced: 0, expected: 0 };
    for h in height_range {
        let block_producer = epoch_manager.get_block_producer_info(epoch_id, h).unwrap();
        if validator == block_producer.account_id() {
            if produced_heights.contains(&h) {
                result.produced += 1;
            }
            result.expected += 1;
        }
    }
    result
}

fn get_num_validator_blocks(
    em_handle: &EpochManagerHandle,
    epoch_id: &EpochId,
    last_known_block_hash: &CryptoHash,
    account_id: &AccountId,
) -> Result<ValidatorStats, EpochError> {
    let epoch_info = em_handle.get_epoch_info(epoch_id)?;
    let validator_id = *epoch_info
        .get_validator_id(account_id)
        .ok_or_else(|| EpochError::NotAValidator(account_id.clone(), *epoch_id))?;
    let aggregator = em_handle.read().get_epoch_info_aggregator_upto_last(last_known_block_hash)?;
    Ok(aggregator
        .block_tracker
        .get(&validator_id)
        .unwrap_or(&ValidatorStats { produced: 0, expected: 0 })
        .clone())
}

#[test]
fn test_num_missing_blocks() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 2;
    let em =
        setup_epoch_manager(validators, epoch_length, 1, 2, 10, 10, 0, default_reward_calculator())
            .into_handle();
    let h = hash_range(8);
    record_block(&mut em.write(), Default::default(), h[0], 0, vec![]);
    record_block(&mut em.write(), h[0], h[1], 1, vec![]);
    record_block(&mut em.write(), h[1], h[3], 3, vec![]);
    let epoch_id = em.get_epoch_id(&h[1]).unwrap();
    assert_eq!(
        get_num_validator_blocks(&em, &epoch_id, &h[3], &"test1".parse().unwrap()).unwrap(),
        count_missing_blocks(&em, &epoch_id, 1..4, &[1, 3], "test1"),
    );
    assert_eq!(
        get_num_validator_blocks(&em, &epoch_id, &h[3], &"test2".parse().unwrap()).unwrap(),
        count_missing_blocks(&em, &epoch_id, 1..4, &[1, 3], "test2"),
    );

    // Build chain 0 <- x <- x <- x <- ( 4 <- 5 ) <- x <- 7
    record_block(&mut em.write(), h[0], h[4], 4, vec![]);
    let epoch_id = em.get_epoch_id(&h[4]).unwrap();
    // Block 4 is first block after genesis and starts new epoch, but we actually count how many missed blocks have happened since block 0.
    assert_eq!(
        get_num_validator_blocks(&em, &epoch_id, &h[4], &"test1".parse().unwrap()).unwrap(),
        count_missing_blocks(&em, &epoch_id, 1..5, &[4], "test1"),
    );
    assert_eq!(
        get_num_validator_blocks(&em, &epoch_id, &h[4], &"test2".parse().unwrap()).unwrap(),
        count_missing_blocks(&em, &epoch_id, 1..5, &[4], "test2"),
    );
    record_block(&mut em.write(), h[4], h[5], 5, vec![]);
    record_block(&mut em.write(), h[5], h[7], 7, vec![]);
    let epoch_id = em.get_epoch_id(&h[7]).unwrap();
    // The next epoch started after 5 with 6, and test2 missed their slot from perspective of block 7.
    assert_eq!(
        get_num_validator_blocks(&em, &epoch_id, &h[7], &"test2".parse().unwrap()).unwrap(),
        count_missing_blocks(&em, &epoch_id, 6..8, &[7], "test2"),
    );
}

/// Test when blocks are all produced, not producing chunks leads to chunk
/// producer kickout.
#[test]
fn test_chunk_producer_kickout() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 10;
    let total_supply = stake_amount * validators.len() as u128;
    let em =
        setup_epoch_manager(validators, epoch_length, 4, 2, 90, 70, 0, default_reward_calculator())
            .into_handle();
    let rng_seed = [0; 32];
    let hashes = hash_range((epoch_length + 2) as usize);
    record_block(&mut em.write(), Default::default(), hashes[0], 0, vec![]);
    let mut expected = 0;
    for (prev_block, (height, curr_block)) in hashes.iter().zip(hashes.iter().enumerate().skip(1)) {
        let height = height as u64;
        let epoch_id = em.get_epoch_id_from_prev_block(prev_block).unwrap();
        let epoch_info = em.get_epoch_info(&epoch_id).unwrap().clone();
        let shard_layout = em.get_shard_layout(&epoch_id).unwrap();
        let chunk_mask = (0..4)
            .map(|shard_index| {
                if height >= epoch_length {
                    return true;
                }
                let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
                let chunk_producer =
                    epoch_info.sample_chunk_producer(&shard_layout, shard_id, height).unwrap();
                // test1 skips chunks
                if chunk_producer == 0 {
                    expected += 1;
                    false
                } else {
                    true
                }
            })
            .collect();

        em.write()
            .record_block_info(
                block_info(
                    *curr_block,
                    height,
                    height - 1,
                    *prev_block,
                    *prev_block,
                    epoch_id.0,
                    chunk_mask,
                    total_supply,
                ),
                rng_seed,
            )
            .unwrap();
    }

    let last_epoch_info =
        hashes.iter().filter_map(|x| em.get_epoch_info(&EpochId(*x)).ok()).next_back();
    assert_eq!(
        last_epoch_info.unwrap().validator_kickout(),
        &[("test1".parse().unwrap(), NotEnoughChunks { produced: 0, expected })]
            .into_iter()
            .collect::<HashMap<_, _>>(),
    );
}

/// Test when all blocks are produced and all chunks are skipped, chunk
/// validator is not kicked out.
#[test]
fn test_chunk_validator_kickout_using_production_stats() {
    let stake_amount = 1_000_000;
    let validators: Vec<(AccountId, Balance)> =
        (0..3).map(|i| (format!("test{i}").parse().unwrap(), stake_amount + 100 - i)).collect();
    let epoch_length = 10;
    let total_supply = stake_amount * validators.len() as u128;
    let num_shards = 2;
    let epoch_config = epoch_config(epoch_length, num_shards, 2, 2, 90, 40, 75);
    let em = EpochManager::new(
        create_test_store(),
        epoch_config,
        default_reward_calculator(),
        validators
            .iter()
            .map(|(account_id, balance)| stake(account_id.clone(), *balance))
            .collect(),
    )
    .unwrap()
    .into_handle();
    let rng_seed = [0; 32];
    let hashes = hash_range((epoch_length + 2) as usize);
    record_block(&mut em.write(), Default::default(), hashes[0], 0, vec![]);
    for (prev_block, (height, curr_block)) in hashes.iter().zip(hashes.iter().enumerate().skip(1)) {
        let height = height as u64;
        let epoch_id = em.get_epoch_id_from_prev_block(prev_block).unwrap();
        let chunk_mask = if height < epoch_length {
            (0..num_shards).map(|i| (height + i) % 2 == 0).collect()
        } else {
            vec![true; num_shards as usize]
        };
        em.write()
            .record_block_info(
                block_info(
                    *curr_block,
                    height,
                    height - 1,
                    *prev_block,
                    *prev_block,
                    epoch_id.0,
                    chunk_mask,
                    total_supply,
                ),
                rng_seed,
            )
            .unwrap();
    }

    let last_epoch_info =
        hashes.iter().filter_map(|x| em.get_epoch_info(&EpochId(*x)).ok()).next_back();
    let total_expected_chunks = num_shards * (epoch_length - 1);
    // Every second chunk is skipped.
    let total_produced_chunks = total_expected_chunks / 2;

    // Chunk producers skip only every second chunk and pass the threshold.
    // Chunk validator validates all chunks, so its performance is determined
    // by the chunk production ratio, which is not enough.
    assert_eq!(
        last_epoch_info.unwrap().validator_kickout(),
        &[(
            "test2".parse().unwrap(),
            NotEnoughChunkEndorsements {
                produced: total_produced_chunks,
                expected: total_expected_chunks
            }
        )]
        .into_iter()
        .collect::<HashMap<_, _>>(),
    );
}

/// Similar to test_chunk_validator_kickout_using_production_stats, however all chunks are produced but
/// but some validators miss chunks and got kicked out.
#[test]
fn test_chunk_validator_kickout_using_endorsement_stats() {
    let stake_amount = 1_000_000;
    let validators: Vec<(AccountId, Balance)> =
        (0..3).map(|i| (format!("test{i}").parse().unwrap(), stake_amount + 100 - i)).collect();
    let epoch_length = 10;
    let total_supply = stake_amount * validators.len() as u128;
    let num_shards = 2;
    let epoch_config = epoch_config(epoch_length, num_shards, 2, 2, 90, 40, 75);
    let em = EpochManager::new(
        create_test_store(),
        epoch_config,
        default_reward_calculator(),
        validators
            .iter()
            .map(|(account_id, balance)| stake(account_id.clone(), *balance))
            .collect(),
    )
    .unwrap()
    .into_handle();
    let rng_seed = [0; 32];
    let hashes = hash_range((epoch_length + 2) as usize);
    record_block(&mut em.write(), Default::default(), hashes[0], 0, vec![]);
    for (prev_block, (height, curr_block)) in hashes.iter().zip(hashes.iter().enumerate().skip(1)) {
        let height = height as u64;
        let epoch_id = em.get_epoch_id_from_prev_block(prev_block).unwrap();
        let shard_layout = em.get_shard_layout(&epoch_id).unwrap();
        // All chunks are produced.
        let chunk_mask = vec![true; num_shards as usize];
        // Prepare the chunk endorsements so that "test2" misses some of the endorsements.
        let mut bitmap = ChunkEndorsementsBitmap::new(num_shards as usize);
        for shard_info in shard_layout.shard_infos() {
            let shard_index = shard_info.shard_index();
            let shard_id = shard_info.shard_id();
            let chunk_validators = em
                .get_chunk_validator_assignments(&epoch_id, shard_id, height)
                .unwrap()
                .ordered_chunk_validators();
            bitmap.add_endorsements(
                shard_index,
                chunk_validators
                    .iter()
                    .map(|account| {
                        account.as_str() != "test2" || (height + shard_index as u64) % 2 == 0
                    })
                    .collect(),
            )
        }
        em.write()
            .record_block_info(
                #[allow(deprecated)]
                BlockInfo::V3(BlockInfoV3 {
                    hash: *curr_block,
                    height,
                    last_finalized_height: height - 1,
                    last_final_block_hash: *prev_block,
                    prev_hash: *prev_block,
                    epoch_id: Default::default(),
                    epoch_first_block: epoch_id.0,
                    proposals: vec![],
                    chunk_mask,
                    latest_protocol_version: PROTOCOL_VERSION,
                    slashed: Default::default(),
                    total_supply,
                    timestamp_nanosec: height * NUM_NS_IN_SECOND,
                    chunk_endorsements: bitmap,
                }),
                rng_seed,
            )
            .unwrap();
    }

    let last_epoch_info =
        hashes.iter().filter_map(|x| em.get_epoch_info(&EpochId(*x)).ok()).next_back();
    let total_expected_chunks = num_shards * (epoch_length - 1);
    // Every second chunk is skipped.
    let total_produced_chunks = total_expected_chunks / 2;

    // Chunk producers produce all chunks, but the chunk validator skips
    // sending endorsements for every second chunk and does not pass the threshold.
    // Chunk validator validates all chunks, so its performance is determined
    // by the chunk production ratio, which is not enough.
    assert_eq!(
        last_epoch_info.unwrap().validator_kickout(),
        &[(
            "test2".parse().unwrap(),
            NotEnoughChunkEndorsements {
                produced: total_produced_chunks,
                expected: total_expected_chunks
            }
        )]
        .into_iter()
        .collect::<HashMap<_, _>>(),
    );
}

#[test]
fn test_compare_epoch_id() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 90, 60).into_handle();
    let h = hash_range(8);
    record_block(&mut epoch_manager.write(), CryptoHash::default(), h[0], 0, vec![]);
    // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
    record_block(
        &mut epoch_manager.write(),
        h[0],
        h[1],
        1,
        vec![stake("test1".parse().unwrap(), 0)],
    );
    record_block(
        &mut epoch_manager.write(),
        h[1],
        h[2],
        2,
        vec![stake("test1".parse().unwrap(), amount_staked)],
    );
    record_block(&mut epoch_manager.write(), h[2], h[3], 3, vec![]);
    let epoch_id0 = epoch_manager.get_epoch_id(&h[0]).unwrap();
    let epoch_id1 = epoch_manager.get_epoch_id(&h[1]).unwrap();
    let epoch_id2 = epoch_manager.get_next_epoch_id(&h[1]).unwrap();
    let epoch_id3 = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
    assert_eq!(epoch_manager.compare_epoch_id(&epoch_id0, &epoch_id1), Ok(Ordering::Equal));
    assert_eq!(epoch_manager.compare_epoch_id(&epoch_id2, &epoch_id3), Ok(Ordering::Less));
    assert_eq!(epoch_manager.compare_epoch_id(&epoch_id3, &epoch_id1), Ok(Ordering::Greater));
    let random_epoch_id = EpochId(hash(&[100]));
    assert!(epoch_manager.compare_epoch_id(&epoch_id3, &random_epoch_id).is_err());
}

#[test]
fn test_fishermen() {
    let stake_amount = 1_000_000;
    let fishermen_threshold = 100;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), fishermen_threshold),
        ("test4".parse().unwrap(), fishermen_threshold / 2),
    ];
    let epoch_length = 4;
    let em =
        setup_epoch_manager(validators, epoch_length, 1, 4, 90, 70, 0, default_reward_calculator());
    let epoch_info = em.get_epoch_info(&EpochId::default()).unwrap();
    check_validators(&epoch_info, &[("test1", stake_amount), ("test2", stake_amount)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), stake_amount),
            ("test2".parse().unwrap(), stake_amount),
            ("test3".parse().unwrap(), 0),
            ("test4".parse().unwrap(), 0),
        ],
    );
    check_kickout(&epoch_info, &[]);
}

#[test]
fn test_fishermen_unstake() {
    let stake_amount = 1_000_000;
    let fishermen_threshold = 100;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), fishermen_threshold),
        ("test3".parse().unwrap(), fishermen_threshold),
    ];
    let mut em = setup_epoch_manager(validators, 2, 1, 1, 90, 70, 0, default_reward_calculator());
    let h = hash_range(5);
    record_block(&mut em, CryptoHash::default(), h[0], 0, vec![]);
    // fishermen unstake
    record_block(&mut em, h[0], h[1], 1, vec![stake("test2".parse().unwrap(), 0)]);
    record_block(&mut em, h[1], h[2], 2, vec![stake("test3".parse().unwrap(), 1)]);

    let epoch_info = em.get_epoch_info(&EpochId(h[2])).unwrap();
    check_validators(&epoch_info, &[("test1", stake_amount)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), stake_amount),
            ("test2".parse().unwrap(), 0),
            ("test3".parse().unwrap(), 0),
        ],
    );
    let kickout = epoch_info.validator_kickout();
    assert!(!kickout.contains_key(AccountIdRef::new_or_panic("test2")));
    matches!(
        kickout.get(AccountIdRef::new_or_panic("test3")),
        Some(ValidatorKickoutReason::NotEnoughStake { .. })
    );
}

#[test]
fn test_validator_consistency() {
    let stake_amount = 1_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 1, 90, 60);
    let h = hash_range(5);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    let epoch_id = epoch_manager.get_epoch_id(&h[0]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    let mut actual_block_producers = HashSet::new();
    for index in epoch_info.block_producers_settlement() {
        let bp = epoch_info.validator_account_id(*index).clone();
        actual_block_producers.insert(bp);
    }
    for index in epoch_info.chunk_producers_settlement().into_iter().flatten() {
        let bp = epoch_info.validator_account_id(*index).clone();
        actual_block_producers.insert(bp);
    }
    for bp in actual_block_producers {
        assert!(epoch_info.account_is_validator(&bp))
    }
}

/// Test that when epoch length is larger than the cache size of block info cache, there is
/// no unexpected error.
#[test]
fn test_finalize_epoch_large_epoch_length() {
    let stake_amount = 1_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let mut epoch_manager =
        setup_default_epoch_manager(validators, (BLOCK_CACHE_SIZE + 1) as u64, 1, 2, 90, 60);
    let h = hash_range(BLOCK_CACHE_SIZE + 2);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..=(BLOCK_CACHE_SIZE + 1) {
        record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
    }
    let epoch_info = epoch_manager.get_epoch_info(&EpochId(h[BLOCK_CACHE_SIZE + 1])).unwrap();
    assert_eq!(
        epoch_info.validators_iter().map(|v| v.account_and_stake()).collect::<Vec<_>>(),
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)],
    );
    assert_eq!(
        epoch_info.stake_change(),
        &change_stake(vec![
            ("test1".parse().unwrap(), stake_amount),
            ("test2".parse().unwrap(), stake_amount)
        ]),
    );
    assert_eq!(
        BLOCK_CACHE_SIZE + 2,
        epoch_manager.epoch_info_aggregator_loop_counter.load(std::sync::atomic::Ordering::SeqCst),
        "Expected every block to be visited exactly once"
    );
}

#[test]
fn test_kickout_set() {
    let stake_amount = 1_000_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), 0),
        ("test3".parse().unwrap(), 10),
    ];
    // have two seats to that 500 would be the threshold
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 90, 60);
    let h = hash_range(5);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![stake("test2".parse().unwrap(), stake_amount)],
    );
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![stake("test2".parse().unwrap(), 0)]);
    let epoch_info1 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    assert_eq!(
        epoch_info1.validators_iter().map(|r| r.account_id().clone()).collect::<Vec<_>>(),
        vec!["test1"]
    );
    assert_eq!(
        epoch_info1.stake_change().clone(),
        change_stake(vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), 0)])
    );
    assert!(epoch_info1.validator_kickout().is_empty());
    record_block(
        &mut epoch_manager,
        h[2],
        h[3],
        3,
        vec![stake("test2".parse().unwrap(), stake_amount)],
    );
    record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
    let epoch_info = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap();
    check_validators(&epoch_info, &[("test1", stake_amount), ("test2", stake_amount)]);
    check_fishermen(&epoch_info, &[]);
    check_kickout(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)],
    );
}

#[test]
fn test_epoch_height_increase() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 90, 60);
    let h = hash_range(5);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[2], 2, vec![stake("test1".parse().unwrap(), 223)]);
    record_block(&mut epoch_manager, h[2], h[4], 4, vec![]);

    let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap();
    assert_ne!(epoch_info2.epoch_height(), epoch_info3.epoch_height());
}

#[test]
fn test_all_kickout_edge_case() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    const EPOCH_LENGTH: u64 = 10;
    let epoch_manager =
        setup_default_epoch_manager(validators, EPOCH_LENGTH, 1, 3, 90, 60).into_handle();
    let hashes = hash_range((8 * EPOCH_LENGTH + 1) as usize);

    record_block(&mut epoch_manager.write(), CryptoHash::default(), hashes[0], 0, vec![]);
    let mut prev_block = hashes[0];
    for (height, curr_block) in hashes.iter().enumerate().skip(1) {
        let height = height as u64;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap().clone();
        let block_producer = epoch_info.sample_block_producer(height);
        let block_producer = epoch_info.validator_account_id(block_producer);
        if height < EPOCH_LENGTH {
            // kickout test2 during first epoch
            if block_producer == "test1" || block_producer == "test3" {
                record_block(
                    &mut epoch_manager.write(),
                    prev_block,
                    *curr_block,
                    height,
                    Vec::new(),
                );
                prev_block = *curr_block;
            }
        } else if height < 2 * EPOCH_LENGTH {
            // produce blocks as normal during the second epoch
            record_block(&mut epoch_manager.write(), prev_block, *curr_block, height, Vec::new());
            prev_block = *curr_block;
        } else if height < 5 * EPOCH_LENGTH {
            // no one produces blocks during epochs 3, 4, 5
            // (but only 2 get kicked out because we can't kickout all)
            ()
        } else if height < 6 * EPOCH_LENGTH {
            // produce blocks normally during epoch 6
            record_block(&mut epoch_manager.write(), prev_block, *curr_block, height, Vec::new());
            prev_block = *curr_block;
        } else if height < 7 * EPOCH_LENGTH {
            // the validator which was not kicked out in epoch 6 stops producing blocks,
            // but cannot be kicked out now because they are the last validator
            if block_producer != epoch_info.validator_account_id(0) {
                record_block(
                    &mut epoch_manager.write(),
                    prev_block,
                    *curr_block,
                    height,
                    Vec::new(),
                );
                prev_block = *curr_block;
            }
        } else {
            // produce blocks normally again
            record_block(&mut epoch_manager.write(), prev_block, *curr_block, height, Vec::new());
            prev_block = *curr_block;
        }
    }

    let last_epoch_info =
        hashes.iter().filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok()).next_back();
    assert_eq!(last_epoch_info.unwrap().validator_kickout(), &HashMap::default());
}

fn check_validators(epoch_info: &EpochInfo, expected_validators: &[(&str, u128)]) {
    for (v, (account_id, stake)) in epoch_info.validators_iter().zip_eq(expected_validators) {
        assert_eq!(v.account_id(), *account_id);
        assert_eq!(v.stake(), *stake);
    }
}

fn check_fishermen(epoch_info: &EpochInfo, expected_fishermen: &[(&str, u128)]) {
    for (v, (account_id, stake)) in epoch_info.fishermen_iter().zip_eq(expected_fishermen) {
        assert_eq!(v.account_id(), *account_id);
        assert_eq!(v.stake(), *stake);
    }
}

fn check_stake_change(epoch_info: &EpochInfo, changes: Vec<(AccountId, u128)>) {
    assert_eq!(epoch_info.stake_change(), &change_stake(changes));
}

fn check_reward(epoch_info: &EpochInfo, changes: Vec<(AccountId, u128)>) {
    assert_eq!(epoch_info.validator_reward(), &reward(changes));
}

fn check_kickout(epoch_info: &EpochInfo, reasons: &[(&str, ValidatorKickoutReason)]) {
    let kickout = reasons
        .into_iter()
        .map(|(account, reason)| (account.parse().unwrap(), reason.clone()))
        .collect::<HashMap<_, _>>();
    assert_eq!(epoch_info.validator_kickout(), &kickout);
}

#[test]
fn test_protocol_version_switch() {
    let store = create_test_store();

    let epoch_config = epoch_config(2, 1, 2, 100, 90, 60, 0).for_protocol_version(PROTOCOL_VERSION);
    let config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (0, Arc::new(epoch_config.clone())),
        (PROTOCOL_VERSION, Arc::new(epoch_config)),
    ]));
    let config = AllEpochConfig::from_epoch_config_store("test-chain", 2, config_store);

    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked),
    ];
    let mut reward_calculator = default_reward_calculator();
    reward_calculator.genesis_protocol_version = 0;
    let mut epoch_manager =
        EpochManager::new(store, config, reward_calculator, validators).unwrap();
    let h = hash_range(8);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..6 {
        let version = if i == 1 { 0 } else { PROTOCOL_VERSION };
        record_block_with_version(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![], version);
    }
    assert_eq!(epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap().protocol_version(), 0);
    assert_eq!(
        epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap().protocol_version(),
        PROTOCOL_VERSION
    );
}

#[test]
fn test_protocol_version_switch_with_shard_layout_change() {
    let store = create_test_store();

    let old_epoch_config =
        epoch_config(2, 1, 2, 100, 90, 60, 0).for_protocol_version(PROTOCOL_VERSION);
    let new_epoch_config =
        epoch_config(2, 4, 2, 100, 90, 60, 0).for_protocol_version(PROTOCOL_VERSION);
    let config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (PROTOCOL_VERSION - 1, Arc::new(old_epoch_config)),
        (PROTOCOL_VERSION, Arc::new(new_epoch_config)),
    ]));
    let config = AllEpochConfig::from_epoch_config_store("test-chain", 2, config_store);

    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked),
    ];
    let mut reward_calculator = default_reward_calculator();
    reward_calculator.genesis_protocol_version = PROTOCOL_VERSION - 1;
    let mut epoch_manager =
        EpochManager::new(store, config, reward_calculator, validators).unwrap();
    let h = hash_range(8);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..8 {
        let version = if i == 1 { PROTOCOL_VERSION - 1 } else { PROTOCOL_VERSION };
        record_block_with_version(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![], version);
    }
    let epochs = [EpochId::default(), EpochId(h[2]), EpochId(h[4])];
    assert_eq!(
        epoch_manager.get_epoch_info(&epochs[1]).unwrap().protocol_version(),
        PROTOCOL_VERSION - 1
    );
    assert_eq!(epoch_manager.get_shard_layout(&epochs[1]).unwrap(), ShardLayout::multi_shard(1, 0));
    assert_eq!(
        epoch_manager.get_epoch_info(&epochs[2]).unwrap().protocol_version(),
        PROTOCOL_VERSION
    );
    assert_eq!(epoch_manager.get_shard_layout(&epochs[2]).unwrap(), ShardLayout::multi_shard(4, 0));

    // Check split shards
    // h[5] is the first block of epoch epochs[1] and shard layout will change at epochs[2]
    let epoch_manager = epoch_manager.into_handle();
    assert_eq!(epoch_manager.will_shard_layout_change(&h[3]).unwrap(), false);
    for i in 4..=5 {
        assert_eq!(epoch_manager.will_shard_layout_change(&h[i]).unwrap(), true);
    }
    assert_eq!(epoch_manager.will_shard_layout_change(&h[6]).unwrap(), false);
}

#[test]
fn test_protocol_version_switch_with_many_seats() {
    let store = create_test_store();
    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked / 5),
    ];

    let config_store = EpochConfigStore::test_single_version(
        PROTOCOL_VERSION,
        TestEpochConfigBuilder::new().epoch_length(10).build(),
    );
    let config = AllEpochConfig::from_epoch_config_store("test-chain", 10, config_store);

    let mut epoch_manager =
        EpochManager::new(store, config, default_reward_calculator(), validators).unwrap();
    let h = hash_range(50);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..32 {
        let version = if i == 1 { 0 } else { PROTOCOL_VERSION };
        record_block_with_version(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![], version);
    }
    assert_eq!(
        epoch_manager.get_epoch_info(&EpochId(h[10])).unwrap().protocol_version(),
        PROTOCOL_VERSION
    );
    assert_eq!(
        epoch_manager.get_epoch_info(&EpochId(h[20])).unwrap().protocol_version(),
        PROTOCOL_VERSION
    );
}

#[test]
fn test_version_switch_kickout_old_version() {
    let store = create_test_store();
    let (version, new_version) = (PROTOCOL_VERSION, PROTOCOL_VERSION + 1);

    let epoch_length = 2;
    let epoch_config =
        epoch_config(epoch_length, 1, 2, 100, 90, 60, 0).for_protocol_version(version);
    let config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (version, Arc::new(epoch_config.clone())),
        (new_version, Arc::new(epoch_config)),
    ]));
    let config = AllEpochConfig::from_epoch_config_store("test-chain", 2, config_store);

    let (large_stake, small_stake) = (1_000, 100);
    let validators = vec![
        stake("test1".parse().unwrap(), large_stake),
        stake("test2".parse().unwrap(), small_stake),
    ];
    let mut reward_calculator = default_reward_calculator();
    reward_calculator.genesis_protocol_version = version;
    let mut epoch_manager =
        EpochManager::new(store, config, reward_calculator, validators).unwrap();

    // Genesis block
    let genesis_hash = test_utils::fake_hash(0);
    record_block(&mut epoch_manager, CryptoHash::default(), genesis_hash, 0, vec![]);

    // First epoch, test1 (with `large_stake`) proposes a new protocol version.
    // As a result, test2 (with `small_stake`) will be kicked out in the next epoch.
    let (mut last_hash, mut height) = (genesis_hash, 1);
    (last_hash, height) =
        record_blocks(&mut epoch_manager, last_hash, height, epoch_length, |_h, validator| {
            (vec![], if validator == "test1" { new_version } else { version })
        });

    // test2 will be kicked out in epoch T+2
    let epoch_info = epoch_manager.get_epoch_info(&EpochId(last_hash)).unwrap();
    check_kickout(
        &epoch_info,
        &[("test2", ProtocolVersionTooOld { version, network_version: new_version })],
    );
    let just_test1 = &[("test1", large_stake)];
    check_validators(&epoch_info, just_test1);

    // Try to add test2 as a proposal in T+1, this should not work.
    (last_hash, _) =
        record_blocks(&mut epoch_manager, last_hash, height, epoch_length, |_h, _validator| {
            (vec![stake("test2".parse().unwrap(), small_stake)], version)
        });

    let epoch_info = epoch_manager.get_epoch_info(&EpochId(last_hash)).unwrap();
    check_validators(&epoch_info, just_test1);
}

/// Epoch aggregator should not need to be recomputed under the following scenario
///                      /-----------h+2
/// h-2 ---- h-1 ------ h
///                      \------h+1
/// even though from the perspective of h+2 the last final block is h-2.
#[test]
fn test_final_block_consistency() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 10, 1, 3, 90, 60);

    let h = hash_range(10);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..5 {
        record_block_with_final_block_hash(
            &mut epoch_manager,
            h[i - 1],
            h[i],
            if i == 1 { CryptoHash::default() } else { h[i - 2] },
            i as u64,
            vec![],
        );
    }

    let epoch_aggregator_final_hash = epoch_manager.epoch_info_aggregator.last_block_hash;

    epoch_manager
        .record_block_info(
            block_info(h[5], 5, 1, h[1], h[2], h[1], vec![], DEFAULT_TOTAL_SUPPLY),
            [0; 32],
        )
        .unwrap()
        .commit()
        .unwrap();
    let new_epoch_aggregator_final_hash = epoch_manager.epoch_info_aggregator.last_block_hash;
    assert_eq!(epoch_aggregator_final_hash, new_epoch_aggregator_final_hash);
}

#[test]
fn test_epoch_validators_cache() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 10, 90, 60);
    let h = hash_range(10);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..4 {
        record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
    }
    assert_eq!(epoch_manager.epoch_validators_ordered.len(), 0);

    let epoch_id = EpochId(h[2]);
    let epoch_validators =
        epoch_manager.get_all_block_producers_settlement(&epoch_id).unwrap().to_vec();
    assert_eq!(epoch_manager.epoch_validators_ordered.len(), 1);
    let epoch_validators_in_cache = epoch_manager.epoch_validators_ordered.get(&epoch_id).unwrap();
    assert_eq!(*epoch_validators, *epoch_validators_in_cache);

    assert_eq!(epoch_manager.epoch_validators_ordered_unique.len(), 0);
    let epoch_validators_unique =
        epoch_manager.get_all_block_producers_ordered(&epoch_id).unwrap().to_vec();
    let epoch_validators_unique_in_cache =
        epoch_manager.epoch_validators_ordered_unique.get(&epoch_id).unwrap();
    assert_eq!(*epoch_validators_unique, *epoch_validators_unique_in_cache);
}

#[test]
fn test_chunk_producers() {
    let amount_staked = 1_000_000;
    // Make sure that last validator has at least 160/1'000'000 of stake.
    // We're running with 2 shards and test1 + test2 has 2'000'000 tokens - so chunk_only should have over 160.
    let validators = vec![
        ("test1".parse().unwrap(), amount_staked),
        ("test2".parse().unwrap(), amount_staked),
        ("chunk_only".parse().unwrap(), 321),
        ("not_enough_producer".parse().unwrap(), 320),
    ];

    // There are 2 shards, and 2 block producers seats.
    // So test1 and test2 should become block producers, and chunk_only should become chunk only producer.
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 2, 2, 90, 60);
    let h = hash_range(10);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..=4 {
        record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
    }

    let epoch_id = EpochId(h[2]);

    let block_producers = epoch_manager
        .get_all_block_producers_settlement(&epoch_id)
        .unwrap()
        .iter()
        .map(|stake| stake.account_id().to_string())
        .collect::<Vec<_>>();
    assert_eq!(vec!(String::from("test1"), String::from("test2")), block_producers);

    let mut chunk_producers = epoch_manager
        .get_all_chunk_producers(&epoch_id)
        .unwrap()
        .to_vec()
        .iter()
        .map(|stake| stake.account_id().to_string())
        .collect::<Vec<_>>();
    chunk_producers.sort();

    assert_eq!(
        vec!(String::from("chunk_only"), String::from("test1"), String::from("test2")),
        chunk_producers
    );
}

#[test]
fn test_validator_kickout_determinism() {
    let mut epoch_config =
        epoch_config(5, 2, 4, 4, 90, 80, 90).for_protocol_version(PROTOCOL_VERSION);
    epoch_config.validator_max_kickout_stake_perc = 99;
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 1000),
        ("test3".parse().unwrap(), 1000),
        ("test4".parse().unwrap(), 500),
        ("test5".parse().unwrap(), 500),
    ];
    let epoch_info = epoch_info(
        0,
        accounts,
        vec![0, 1, 2, 3],
        vec![vec![0, 1, 2], vec![0, 1, 3]],
        PROTOCOL_VERSION,
    );
    let block_validator_tracker = HashMap::from([
        (0, ValidatorStats { produced: 100, expected: 100 }),
        (1, ValidatorStats { produced: 90, expected: 100 }),
        (2, ValidatorStats { produced: 100, expected: 100 }),
        (3, ValidatorStats { produced: 89, expected: 100 }),
    ]);
    let chunk_stats0 = Vec::from([
        (0, ChunkStats::new_with_production(100, 100)),
        (
            1,
            ChunkStats {
                production: ValidatorStats { produced: 80, expected: 100 },
                // Note that test1 would not pass chunk endorsement
                // threshold, but it is applied to nodes which are only
                // chunk validators.
                endorsement: ValidatorStats { produced: 0, expected: 100 },
            },
        ),
        (2, ChunkStats::new_with_production(70, 100)),
        (5, ChunkStats::new_with_endorsement(91, 100)),
    ]);
    let chunk_stats1 = Vec::from([
        (0, ChunkStats::new_with_production(70, 100)),
        (
            1,
            ChunkStats {
                production: ValidatorStats { produced: 81, expected: 100 },
                endorsement: ValidatorStats { produced: 1, expected: 100 },
            },
        ),
        (3, ChunkStats::new_with_production(100, 100)),
        // test4 is only a chunk validator and should be kicked out.
        (4, ChunkStats::new_with_endorsement(89, 100)),
    ]);
    let chunk_stats_tracker1 = HashMap::from([
        (ShardId::new(0), chunk_stats0.clone().into_iter().collect()),
        (ShardId::new(1), chunk_stats1.clone().into_iter().collect()),
    ]);
    let chunk_stats0 = chunk_stats0.into_iter().rev();
    let chunk_stats_tracker2 = HashMap::from([
        (ShardId::new(0), chunk_stats0.collect()),
        (ShardId::new(1), chunk_stats1.into_iter().collect()),
    ]);
    let (_validator_stats, kickouts1) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_validator_tracker,
        &chunk_stats_tracker1,
        &HashMap::new(),
    );
    let (_validator_stats, kickouts2) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_validator_tracker,
        &chunk_stats_tracker2,
        &HashMap::new(),
    );
    assert_eq!(kickouts1, kickouts2);
}

/// Tests the scenario that there are two chunk validators (test2 and test3) with different endorsement ratio, and
/// so the validator with the lower endorsement ratio is kicked out.
#[test]
fn test_chunk_validators_with_different_endorsement_ratio() {
    let mut epoch_config =
        epoch_config(5, 2, 2, 2, 90, 90, 70).for_protocol_version(PROTOCOL_VERSION);
    // Set the max kickout stake percentage so that only one of the chunk validators
    // is kicked out, and the other chunk validator is exempted from kickout.
    // Both chunk validators have endorsement ratio lower than the kickout threshold.
    epoch_config.validator_max_kickout_stake_perc = 30;
    // Test 0-1 are block+chunk producers and 2-3 are chunk validators only.
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 500),
        ("test3".parse().unwrap(), 500),
    ];
    let epoch_info = epoch_info(
        0,
        accounts,
        vec![0, 1, 2, 3],
        vec![vec![0, 1, 2], vec![0, 1, 3]],
        PROTOCOL_VERSION,
    );
    let block_validator_tracker = HashMap::from([
        (0, ValidatorStats { produced: 100, expected: 100 }),
        (1, ValidatorStats { produced: 100, expected: 100 }),
    ]);
    let chunk_stats0 = Vec::from([
        (0, ChunkStats::new_with_production(100, 100)),
        (1, ChunkStats::new_with_production(100, 100)),
        (2, ChunkStats::new_with_endorsement(65, 100)),
    ]);
    let chunk_stats1 = Vec::from([
        (0, ChunkStats::new_with_production(100, 100)),
        (1, ChunkStats::new_with_production(100, 100)),
        (3, ChunkStats::new_with_endorsement(60, 100)),
    ]);
    let chunk_stats_tracker = HashMap::from([
        (ShardId::new(0), chunk_stats0.into_iter().collect()),
        (ShardId::new(1), chunk_stats1.into_iter().collect()),
    ]);
    let (_validator_stats, kickouts) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_validator_tracker,
        &chunk_stats_tracker,
        &HashMap::new(),
    );
    assert_eq!(
        kickouts,
        HashMap::from([(
            "test3".parse().unwrap(),
            NotEnoughChunkEndorsements { produced: 60, expected: 100 }
        ),])
    );
}

/// Tests the scenario that there are two chunk validators (test2 and test3) have the same online ratio but different stake,
/// so the validator with the lower stake is kicked out.
#[test]
fn test_chunk_validators_with_same_endorsement_ratio_and_different_stake() {
    let mut epoch_config =
        epoch_config(5, 2, 2, 2, 90, 90, 70).for_protocol_version(PROTOCOL_VERSION);
    // Set the max kickout stake percentage so that only one of the chunk validators
    // is kicked out, and the other chunk validator is exempted from kickout.
    // Both chunk validators have endorsement ratio lower than the kickout threshold.
    epoch_config.validator_max_kickout_stake_perc = 30;
    // Test 0-1 are block+chunk producers and 2-3 are chunk validators only.
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 500),
        ("test3".parse().unwrap(), 499),
    ];
    let epoch_info = epoch_info(
        0,
        accounts,
        vec![0, 1, 2, 3],
        vec![vec![0, 1, 2], vec![0, 1, 3]],
        PROTOCOL_VERSION,
    );
    let block_validator_tracker = HashMap::from([
        (0, ValidatorStats { produced: 100, expected: 100 }),
        (1, ValidatorStats { produced: 100, expected: 100 }),
    ]);
    let chunk_stats0 = Vec::from([
        (0, ChunkStats::new_with_production(100, 100)),
        (1, ChunkStats::new_with_production(100, 100)),
        (2, ChunkStats::new_with_endorsement(65, 100)),
    ]);
    let chunk_stats1 = Vec::from([
        (0, ChunkStats::new_with_production(100, 100)),
        (1, ChunkStats::new_with_production(100, 100)),
        (3, ChunkStats::new_with_endorsement(65, 100)),
    ]);
    let chunk_stats_tracker = HashMap::from([
        (ShardId::new(0), chunk_stats0.into_iter().collect()),
        (ShardId::new(1), chunk_stats1.into_iter().collect()),
    ]);
    let (_validator_stats, kickouts) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_validator_tracker,
        &chunk_stats_tracker,
        &HashMap::new(),
    );
    assert_eq!(
        kickouts,
        HashMap::from([(
            "test3".parse().unwrap(),
            NotEnoughChunkEndorsements { produced: 65, expected: 100 }
        ),])
    );
}

/// Tests the scenario that there are two chunk validators (test2 and test3) have the same online ratio and stake,
/// so we select the exempted validator based on the ordering of the account id.
#[test]
fn test_chunk_validators_with_same_endorsement_ratio_and_stake() {
    let mut epoch_config =
        epoch_config(5, 2, 2, 2, 90, 90, 70).for_protocol_version(PROTOCOL_VERSION);
    // Set the max kickout stake percentage so that only one of the chunk validators
    // is kicked out, and the other chunk validator is exempted from kickout.
    // Both chunk validators have endorsement ratio lower than the kickout threshold.
    epoch_config.validator_max_kickout_stake_perc = 30;
    // Test 0-1 are block+chunk producers and 2-3 are chunk validators only.
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 500),
        ("test3".parse().unwrap(), 500),
    ];
    let epoch_info = epoch_info(
        0,
        accounts,
        vec![0, 1, 2, 3],
        vec![vec![0, 1, 2], vec![0, 1, 3]],
        PROTOCOL_VERSION,
    );
    let block_validator_tracker = HashMap::from([
        (0, ValidatorStats { produced: 100, expected: 100 }),
        (1, ValidatorStats { produced: 100, expected: 100 }),
    ]);
    let chunk_stats0 = Vec::from([
        (0, ChunkStats::new_with_production(100, 100)),
        (1, ChunkStats::new_with_production(100, 100)),
        (2, ChunkStats::new_with_endorsement(65, 100)),
    ]);
    let chunk_stats1 = Vec::from([
        (0, ChunkStats::new_with_production(100, 100)),
        (1, ChunkStats::new_with_production(100, 100)),
        (3, ChunkStats::new_with_endorsement(65, 100)),
    ]);
    let chunk_stats_tracker = HashMap::from([
        (ShardId::new(0), chunk_stats0.into_iter().collect()),
        (ShardId::new(1), chunk_stats1.into_iter().collect()),
    ]);
    let (_validator_stats, kickouts) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_validator_tracker,
        &chunk_stats_tracker,
        &HashMap::new(),
    );
    assert_eq!(
        kickouts,
        HashMap::from([(
            "test2".parse().unwrap(),
            NotEnoughChunkEndorsements { produced: 65, expected: 100 }
        ),])
    );
}

/// A sanity test for the compute_validators_to_reward_and_kickout function,
/// checks that validators that don't meet their kickout thresholds are kicked out.
#[test]
fn test_validator_kickout_sanity() {
    let epoch_config = epoch_config(5, 2, 4, 4, 90, 80, 90).for_protocol_version(PROTOCOL_VERSION);
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 1000),
        ("test3".parse().unwrap(), 1000),
        ("test4".parse().unwrap(), 500),
        ("test5".parse().unwrap(), 500),
    ];
    let epoch_info = epoch_info(
        0,
        accounts,
        vec![0, 1, 2, 3],
        vec![vec![0, 1, 2], vec![0, 1, 3]],
        PROTOCOL_VERSION,
    );
    let block_validator_tracker = HashMap::from([
        (0, ValidatorStats { produced: 100, expected: 100 }),
        (1, ValidatorStats { produced: 90, expected: 100 }),
        (2, ValidatorStats { produced: 100, expected: 100 }),
        (3, ValidatorStats { produced: 89, expected: 100 }),
    ]);
    let chunk_stats_tracker = HashMap::from([
        (
            ShardId::new(0),
            HashMap::from([
                (0, ChunkStats::new_with_production(100, 100)),
                (
                    1,
                    ChunkStats {
                        production: ValidatorStats { produced: 80, expected: 100 },
                        // Note that test1 would not pass chunk endorsement
                        // threshold, but it is applied to nodes which are only
                        // chunk validators.
                        endorsement: ValidatorStats { produced: 0, expected: 100 },
                    },
                ),
                (2, ChunkStats::new_with_production(70, 100)),
                (5, ChunkStats::new_with_endorsement(91, 100)),
            ]),
        ),
        (
            ShardId::new(1),
            HashMap::from([
                (0, ChunkStats::new_with_production(70, 100)),
                (
                    1,
                    ChunkStats {
                        production: ValidatorStats { produced: 81, expected: 100 },
                        endorsement: ValidatorStats { produced: 1, expected: 100 },
                    },
                ),
                (3, ChunkStats::new_with_production(100, 100)),
                // test4 is only a chunk validator and should be kicked out.
                (4, ChunkStats::new_with_endorsement(89, 100)),
            ]),
        ),
    ]);
    let (validator_stats, kickouts) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_validator_tracker,
        &chunk_stats_tracker,
        &HashMap::new(),
    );
    assert_eq!(
        kickouts,
        HashMap::from([
            ("test2".parse().unwrap(), NotEnoughChunks { produced: 70, expected: 100 }),
            ("test3".parse().unwrap(), NotEnoughBlocks { produced: 89, expected: 100 }),
            ("test4".parse().unwrap(), NotEnoughChunkEndorsements { produced: 89, expected: 100 }),
        ])
    );
    let expected_validator_stats: HashMap<AccountId, _> = HashMap::from([
        (
            "test0".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 100, expected: 100 },
                chunk_stats: ChunkStats::new_with_production(170, 200),
            },
        ),
        (
            "test1".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 90, expected: 100 },
                chunk_stats: ChunkStats {
                    production: ValidatorStats { produced: 161, expected: 200 },
                    endorsement: ValidatorStats { produced: 1, expected: 200 },
                },
            },
        ),
        (
            "test2".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 100, expected: 100 },
                chunk_stats: ChunkStats::new_with_production(70, 100),
            },
        ),
        (
            "test3".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 89, expected: 100 },
                chunk_stats: ChunkStats::new_with_production(100, 100),
            },
        ),
        (
            "test4".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 0, expected: 0 },
                chunk_stats: ChunkStats::new_with_endorsement(89, 100),
            },
        ),
        (
            "test5".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 0, expected: 0 },
                chunk_stats: ChunkStats::new_with_endorsement(91, 100),
            },
        ),
    ]);
    assert_eq!(
        validator_stats.keys().sorted().collect_vec(),
        expected_validator_stats.keys().sorted().collect_vec()
    );
    for account_id in validator_stats.keys() {
        assert_eq!(
            validator_stats.get(account_id).unwrap(),
            expected_validator_stats.get(account_id).unwrap(),
            "Validator stats mismatch for account_id: {account_id}"
        );
    }
}

/// We include some validators that are both block/chunk producers and also chunk validators
/// as well as some validators that are only chunk validators.
/// This test does not test kickouts at all.
#[test]
fn test_chunk_endorsement_stats() {
    let epoch_config = epoch_config(5, 2, 4, 100, 90, 80, 0).for_protocol_version(PROTOCOL_VERSION);
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 1000),
        ("test3".parse().unwrap(), 1000),
    ];
    let epoch_info = epoch_info(
        0,
        accounts,
        vec![0, 1, 2, 3],
        vec![vec![0, 1, 2], vec![0, 1, 3]],
        PROTOCOL_VERSION,
    );
    let (validator_stats, kickouts) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &HashMap::from([
            (0, ValidatorStats { produced: 100, expected: 100 }),
            (1, ValidatorStats { produced: 90, expected: 100 }),
        ]),
        &HashMap::from([
            (
                ShardId::new(0),
                HashMap::from([
                    (0, ChunkStats::new(100, 100, 100, 100)),
                    (1, ChunkStats::new(90, 100, 100, 100)),
                    (2, ChunkStats::new_with_endorsement(100, 100)),
                    (3, ChunkStats::new_with_endorsement(95, 100)),
                ]),
            ),
            (
                ShardId::new(1),
                HashMap::from([
                    (0, ChunkStats::new(95, 100, 100, 100)),
                    (1, ChunkStats::new(95, 100, 90, 100)),
                    (2, ChunkStats::new_with_endorsement(95, 100)),
                    (3, ChunkStats::new_with_endorsement(90, 100)),
                ]),
            ),
        ]),
        &HashMap::new(),
    );
    assert_eq!(kickouts, HashMap::new(),);
    assert_eq!(
        validator_stats,
        HashMap::from([
            (
                "test0".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 100, expected: 100 },
                    chunk_stats: ChunkStats::new(195, 200, 200, 200),
                }
            ),
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 90, expected: 100 },
                    chunk_stats: ChunkStats::new(185, 200, 190, 200),
                }
            ),
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_endorsement(195, 200),
                }
            ),
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_endorsement(185, 200),
                }
            ),
        ])
    );
}

#[test]
/// Test that the stake of validators kicked out in an epoch doesn't exceed the max_kickout_stake_ratio
fn test_max_kickout_stake_ratio() {
    let mut epoch_config =
        epoch_config(5, 2, 4, 100, 90, 80, 0).for_protocol_version(PROTOCOL_VERSION);
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 1000),
        ("test3".parse().unwrap(), 1000),
        ("test4".parse().unwrap(), 1000),
    ];
    let epoch_info =
        epoch_info(0, accounts, vec![0, 1, 2, 3], vec![vec![0, 1], vec![2, 4]], PROTOCOL_VERSION);
    let block_stats = HashMap::from([
        (0, ValidatorStats { produced: 50, expected: 100 }),
        // here both test1 and test2 produced the most number of blocks, we made that intentionally
        // to test the algorithm to pick one deterministically to save in this case.
        (1, ValidatorStats { produced: 70, expected: 100 }),
        (2, ValidatorStats { produced: 70, expected: 100 }),
        // validator 3 doesn't need to produce any block or chunk
        (3, ValidatorStats { produced: 0, expected: 0 }),
    ]);
    let chunk_stats_tracker = HashMap::from([
        (
            ShardId::new(0),
            HashMap::from([
                (0, ChunkStats::new_with_production(0, 100)),
                (1, ChunkStats::new_with_production(0, 100)),
            ]),
        ),
        (
            ShardId::new(1),
            HashMap::from([
                (2, ChunkStats::new_with_production(100, 100)),
                (4, ChunkStats::new_with_production(50, 100)),
            ]),
        ),
    ]);
    let prev_validator_kickout =
        HashMap::from([("test3".parse().unwrap(), ValidatorKickoutReason::Unstaked)]);
    let (validator_stats, kickouts) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_stats,
        &chunk_stats_tracker,
        &prev_validator_kickout,
    );
    assert_eq!(
        kickouts,
        // We would have kicked out test0, test1, test2 and test4, but test3 was kicked out
        // last epoch. To avoid kicking out all validators in two epochs, we saved test1 because
        // it produced the most blocks (test1 and test2 produced the same number of blocks, but test1
        // is listed before test2 in the validators list).
        HashMap::from([
            ("test0".parse().unwrap(), NotEnoughBlocks { produced: 50, expected: 100 }),
            ("test2".parse().unwrap(), NotEnoughBlocks { produced: 70, expected: 100 }),
            ("test4".parse().unwrap(), NotEnoughChunks { produced: 50, expected: 100 }),
        ])
    );
    let wanted_validator_stats = HashMap::from([
        (
            "test0".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 50, expected: 100 },
                chunk_stats: ChunkStats::new_with_production(0, 100),
            },
        ),
        (
            "test1".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 70, expected: 100 },
                chunk_stats: ChunkStats::new_with_production(0, 100),
            },
        ),
        (
            "test2".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 70, expected: 100 },
                chunk_stats: ChunkStats::new_with_production(100, 100),
            },
        ),
        (
            "test3".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 0, expected: 0 },
                chunk_stats: ChunkStats::default(),
            },
        ),
        (
            "test4".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 0, expected: 0 },
                chunk_stats: ChunkStats::new_with_production(50, 100),
            },
        ),
    ]);
    assert_eq!(validator_stats, wanted_validator_stats,);
    // At most 40% of total stake can be kicked out
    epoch_config.validator_max_kickout_stake_perc = 40;
    let (validator_stats, kickouts) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_stats,
        &chunk_stats_tracker,
        &prev_validator_kickout,
    );
    assert_eq!(
        kickouts,
        // We would have kicked out test0, test1, test2 and test4, but
        // test1, test2, and test4 are exempted. Note that test3 can't be exempted because it
        // is in prev_validator_kickout.
        HashMap::from([(
            "test0".parse().unwrap(),
            NotEnoughBlocks { produced: 50, expected: 100 }
        ),])
    );
    assert_eq!(validator_stats, wanted_validator_stats,);
}

/// Common test scenario for a couple of tests exercising chunk validator kickouts.
fn test_chunk_validator_kickout(expected_kickouts: HashMap<AccountId, ValidatorKickoutReason>) {
    let mut epoch_config =
        epoch_config(5, 2, 4, 100, 80, 80, 80).for_protocol_version(PROTOCOL_VERSION);
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 1000),
        ("test3".parse().unwrap(), 1000),
        ("test4".parse().unwrap(), 1000),
        ("test5".parse().unwrap(), 1000),
    ];
    let epoch_info =
        epoch_info(0, accounts, vec![0, 1, 2, 3], vec![vec![0, 1], vec![0, 2]], PROTOCOL_VERSION);
    let block_stats = HashMap::from([
        (0, ValidatorStats { produced: 90, expected: 100 }),
        (1, ValidatorStats { produced: 90, expected: 100 }),
        (2, ValidatorStats { produced: 90, expected: 100 }),
        (3, ValidatorStats { produced: 0, expected: 0 }),
    ]);
    let chunk_stats_tracker = HashMap::from([
        (
            ShardId::new(0),
            HashMap::from([
                (0, ChunkStats::new_with_production(90, 100)),
                (1, ChunkStats::new_with_production(90, 100)),
                (3, ChunkStats::new_with_endorsement(0, 0)),
                (4, ChunkStats::new_with_endorsement(10, 100)),
                (5, ChunkStats::new_with_endorsement(90, 100)),
            ]),
        ),
        (
            ShardId::new(1),
            HashMap::from([
                (0, ChunkStats::new_with_production(90, 100)),
                (2, ChunkStats::new_with_production(90, 100)),
                (3, ChunkStats::new_with_endorsement(0, 0)),
                (4, ChunkStats::new_with_endorsement(10, 100)),
                (5, ChunkStats::new_with_endorsement(90, 100)),
            ]),
        ),
    ]);

    let prev_validator_kickout =
        HashMap::from([("test3".parse().unwrap(), ValidatorKickoutReason::Unstaked)]);
    // At most 40% of total stake can be kicked out
    epoch_config.validator_max_kickout_stake_perc = 40;
    let (_, kickouts) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_stats,
        &chunk_stats_tracker,
        &prev_validator_kickout,
    );
    assert_eq!(kickouts, expected_kickouts);
}

#[test]
/// Tests the case where a chunk validator has low endorsement stats and is kicked out (not exempted).
/// In this test, first 3 accounts are block and chunk producers and next 2 are chunk validator only.
fn test_chunk_validator_kicked_out_for_low_endorsement() {
    test_chunk_validator_kickout(HashMap::from([(
        "test4".parse().unwrap(),
        NotEnoughChunkEndorsements { produced: 20, expected: 200 },
    )]));
}

#[test]
/// Tests that a validator is not kicked out due to low endorsement only (as long as it produces most of its blocks and chunks).
fn test_block_and_chunk_producer_not_kicked_out_for_low_endorsements() {
    let mut epoch_config =
        epoch_config(5, 2, 4, 100, 80, 80, 80).for_protocol_version(PROTOCOL_VERSION);
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 1000),
    ];
    let epoch_info = epoch_info(
        0,
        accounts,
        vec![0, 1, 2],
        vec![vec![0, 1, 2], vec![0, 1, 2]],
        PROTOCOL_VERSION,
    );
    let block_stats = HashMap::from([
        (0, ValidatorStats { produced: 90, expected: 100 }),
        (1, ValidatorStats { produced: 90, expected: 100 }),
        (2, ValidatorStats { produced: 90, expected: 100 }),
    ]);
    let chunk_stats_tracker = HashMap::from([
        (
            ShardId::new(0),
            HashMap::from([
                (0, ChunkStats::new(90, 100, 10, 100)),
                (1, ChunkStats::new(90, 100, 10, 100)),
                (2, ChunkStats::new(90, 100, 10, 100)),
            ]),
        ),
        (
            ShardId::new(1),
            HashMap::from([
                (0, ChunkStats::new(90, 100, 10, 100)),
                (1, ChunkStats::new(90, 100, 10, 100)),
                (2, ChunkStats::new(90, 100, 10, 100)),
            ]),
        ),
    ]);

    // At most 40% of total stake can be kicked out
    epoch_config.validator_max_kickout_stake_perc = 40;
    let (_, kickouts) = EpochManager::compute_validators_to_reward_and_kickout(
        &epoch_config,
        &epoch_info,
        &block_stats,
        &chunk_stats_tracker,
        &HashMap::new(),
    );
    assert_eq!(kickouts, HashMap::new());
}

fn test_chunk_header(h: &[CryptoHash], signer: &ValidatorSigner) -> ShardChunkHeader {
    ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        h[0],
        h[2],
        h[2],
        h[2],
        0,
        1,
        ShardId::new(0),
        0,
        0,
        0,
        h[2],
        h[2],
        vec![],
        Default::default(),
        BandwidthRequests::empty(),
        signer,
    ))
}

#[test]
fn test_verify_partial_witness_signature() {
    use near_crypto::Signature;
    use near_primitives::test_utils::create_test_signer;
    use std::str::FromStr;

    let amount_staked = 1_000_000;
    let account_id = AccountId::from_str("test1").unwrap();
    let validators = vec![(account_id.clone(), amount_staked)];
    let h = hash_range(6);

    let mut epoch_manager = setup_default_epoch_manager(validators, 5, 1, 2, 90, 60);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![]);

    let epoch_manager = epoch_manager.into_handle();
    let epoch_id = epoch_manager.get_epoch_id(&h[1]).unwrap();

    // Verify if the test signer has same public key as the chunk validator.
    let validator = epoch_manager.get_validator_by_account_id(&epoch_id, &account_id).unwrap();
    let chunk_producer: AccountId = "test1".parse().unwrap();
    let signer = Arc::new(create_test_signer(chunk_producer.as_str()));
    assert_eq!(signer.public_key(), validator.public_key().clone());

    // Build a chunk state witness with arbitrary data.
    let chunk_header = test_chunk_header(&h, signer.as_ref());
    let mut partial_witness = PartialEncodedStateWitness::new(
        epoch_id,
        chunk_header.clone(),
        0,
        "witness".bytes().collect(),
        7,
        signer.as_ref(),
    );
    let chunk_producer =
        epoch_manager.get_chunk_producer_info(&partial_witness.chunk_production_key()).unwrap();
    assert!(partial_witness.verify(chunk_producer.public_key()));

    // Check invalid chunk state witness signature.
    partial_witness.signature = Signature::default();
    assert!(!partial_witness.verify(chunk_producer.public_key()));

    // Check chunk state witness invalidity when signer is not a chunk validator.
    let bad_signer = Arc::new(create_test_signer("test2"));
    let bad_partial_witness = PartialEncodedStateWitness::new(
        epoch_id,
        chunk_header,
        0,
        "witness".bytes().collect(),
        7,
        bad_signer.as_ref(),
    );
    assert!(!bad_partial_witness.verify(chunk_producer.public_key()));
}

/// Simulate the blockchain over a few epochs and verify that possible_epochs_of_height_around_tip()
/// gives the correct results at each step.
/// Some of the blocks are missing to make the test more interesting.
/// The blocks present in each epoch are:
/// Epoch(111): genesis
/// Epoch(111): 1, 2, 3, 4, 5
///     epoch1: 6, 7, 8, 9, 10
///     epoch2: 12, 14, 16, 18, 20, 22, 24, 25, 26
///     epoch3: 27, 28, 29, 30, 31
///     epoch4: 32+
#[test]
fn test_possible_epochs_of_height_around_tip() {
    use std::str::FromStr;
    init_test_logger();

    let amount_staked = 1_000_000;
    let account_id = AccountId::from_str("test1").unwrap();
    let validators = vec![(account_id, amount_staked)];
    let h = hash_range(50);

    let genesis_epoch = EpochId(CryptoHash::default());

    let epoch_length = 5;
    let epoch_manager =
        setup_default_epoch_manager(validators, epoch_length, 1, 2, 90, 60).into_handle();

    // Add the genesis block with height 1000
    let genesis_height = 1000;
    record_block(&mut epoch_manager.write(), CryptoHash::default(), h[0], genesis_height, vec![]);

    let genesis_tip = Tip {
        height: genesis_height,
        last_block_hash: h[0],
        prev_block_hash: CryptoHash::default(),
        epoch_id: genesis_epoch,
        next_epoch_id: genesis_epoch,
    };

    assert_eq!(
        epoch_manager.possible_epochs_of_height_around_tip(&genesis_tip, 0).unwrap(),
        vec![]
    );
    assert_eq!(
        epoch_manager
            .possible_epochs_of_height_around_tip(&genesis_tip, genesis_height - 1)
            .unwrap(),
        vec![]
    );
    assert_eq!(
        epoch_manager.possible_epochs_of_height_around_tip(&genesis_tip, genesis_height).unwrap(),
        vec![genesis_epoch]
    );
    assert_eq!(
        epoch_manager
            .possible_epochs_of_height_around_tip(&genesis_tip, genesis_height + 1)
            .unwrap(),
        vec![genesis_epoch]
    );
    assert_eq!(
        epoch_manager.possible_epochs_of_height_around_tip(&genesis_tip, 10000000).unwrap(),
        vec![genesis_epoch]
    );

    let epoch1 = EpochId(h[0]);
    tracing::info!(target: "test", ?epoch1);

    // Add blocks with heights 1..5, a standard epoch with no surprises
    for i in 1..=5 {
        let height = genesis_height + i as BlockHeight;
        tracing::info!(target: "test", height);
        record_block(&mut epoch_manager.write(), h[i - 1], h[i], height, vec![]);
        let tip = Tip {
            height,
            last_block_hash: h[i],
            prev_block_hash: h[i - 1],
            epoch_id: genesis_epoch,
            next_epoch_id: epoch1,
        };
        assert_eq!(epoch_manager.possible_epochs_of_height_around_tip(&tip, 0).unwrap(), vec![]);
        assert_eq!(
            epoch_manager.possible_epochs_of_height_around_tip(&tip, genesis_height).unwrap(),
            vec![genesis_epoch]
        );
        assert_eq!(
            epoch_manager.possible_epochs_of_height_around_tip(&tip, genesis_height + 1).unwrap(),
            vec![genesis_epoch]
        );
        for h in 1..=5 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h as BlockHeight)
                    .unwrap(),
                vec![genesis_epoch]
            );
        }
        assert_eq!(
            epoch_manager.possible_epochs_of_height_around_tip(&tip, genesis_height + 6).unwrap(),
            vec![genesis_epoch, epoch1]
        );
        assert_eq!(
            epoch_manager.possible_epochs_of_height_around_tip(&tip, 1000000).unwrap(),
            vec![genesis_epoch, epoch1]
        );
    }

    let epoch2 = EpochId(h[5]);
    tracing::info!(target: "test", ?epoch2);

    // Add blocks with heights 6..10, also a standard epoch with no surprises
    for i in 6..=10 {
        let height = genesis_height + i as BlockHeight;
        tracing::info!(target: "test", height);
        record_block(&mut epoch_manager.write(), h[i - 1], h[i], height, vec![]);
        let tip = Tip {
            height,
            last_block_hash: h[i],
            prev_block_hash: h[i - 1],
            epoch_id: epoch1,
            next_epoch_id: epoch2,
        };
        assert_eq!(epoch_manager.possible_epochs_of_height_around_tip(&tip, 0).unwrap(), vec![]);
        assert_eq!(
            epoch_manager.possible_epochs_of_height_around_tip(&tip, genesis_height).unwrap(),
            vec![]
        );
        for h in 1..=5 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![genesis_epoch]
            );
        }
        for h in 6..=10 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h as BlockHeight)
                    .unwrap(),
                vec![epoch1]
            );
        }
        assert_eq!(
            epoch_manager.possible_epochs_of_height_around_tip(&tip, genesis_height + 11).unwrap(),
            vec![epoch1, epoch2]
        );
        assert_eq!(
            epoch_manager.possible_epochs_of_height_around_tip(&tip, 1000000).unwrap(),
            vec![epoch1, epoch2]
        );
    }

    let epoch3 = EpochId(h[10]);
    tracing::info!(target: "test", ?epoch3);

    // Now there is a very long epoch with no final blocks (all odd blocks are missing)
    // For all the blocks inside this for the last final block will be block #8, as it has #9 and #10
    // on top of it.
    let last_final_block_hash = h[8];
    let last_finalized_height = genesis_height + 8;
    for i in (12..=24).filter(|i| i % 2 == 0) {
        let height = genesis_height + i as BlockHeight;
        tracing::info!(target: "test", height);
        let block_info = block_info(
            h[i],
            height,
            last_finalized_height,
            last_final_block_hash,
            h[i - 2],
            h[12],
            vec![],
            DEFAULT_TOTAL_SUPPLY,
        );
        epoch_manager.write().record_block_info(block_info, [0; 32]).unwrap().commit().unwrap();
        let tip = Tip {
            height,
            last_block_hash: h[i],
            prev_block_hash: h[i - 2],
            epoch_id: epoch2,
            next_epoch_id: epoch3,
        };
        for h in 0..=5 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![]
            );
        }
        for h in 6..=10 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![epoch1]
            );
        }
        // Block 11 isn't in any epoch. Block 10 was the last of the previous epoch and block 12
        // is the first one of the new epoch. Block 11 was skipped and doesn't belong to any epoch.
        assert_eq!(
            epoch_manager.possible_epochs_of_height_around_tip(&tip, genesis_height + 11).unwrap(),
            vec![]
        );
        for h in 12..17 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![epoch2]
            );
        }
        for h in 17..=24 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![epoch2, epoch3]
            );
        }
    }

    // Finally there are two consecutive blocks on top of block 24 which
    // make block 24 final and finalize epoch2.
    for i in [25, 26] {
        let height = genesis_height + i as BlockHeight;
        tracing::info!(target: "test", height);
        let block_info = block_info(
            h[i],
            height,
            if i == 26 { genesis_height + 24 } else { last_finalized_height },
            if i == 26 { h[24] } else { last_final_block_hash },
            h[i - 1],
            h[12],
            vec![],
            DEFAULT_TOTAL_SUPPLY,
        );
        epoch_manager.write().record_block_info(block_info, [0; 32]).unwrap().commit().unwrap();
        let tip = Tip {
            height,
            last_block_hash: h[i],
            prev_block_hash: h[i - 1],
            epoch_id: epoch2,
            next_epoch_id: epoch3,
        };
        for h in 0..=5 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![]
            );
        }
        for h in 6..=10 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![epoch1]
            );
        }
        // Block 11 isn't in any epoch. Block 10 was the last of the previous epoch and block 12
        // is the first one of the new epoch. Block 11 was skipped and doesn't belong to any epoch.
        assert_eq!(
            epoch_manager.possible_epochs_of_height_around_tip(&tip, genesis_height + 11).unwrap(),
            vec![]
        );
        for h in 12..17 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![epoch2]
            );
        }
        for h in 17..=26 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![epoch2, epoch3]
            );
        }
    }

    // One more epoch without surprises to make sure that the previous weird epoch is handled correctly
    let epoch4 = EpochId(h[12]);
    for i in 27..=31 {
        let height = genesis_height + i as BlockHeight;
        tracing::info!(target: "test", height);
        record_block(&mut epoch_manager.write(), h[i - 1], h[i], height, vec![]);
        let tip = Tip {
            height,
            last_block_hash: h[i],
            prev_block_hash: h[i - 1],
            epoch_id: epoch3,
            next_epoch_id: epoch4,
        };
        assert_eq!(epoch_manager.possible_epochs_of_height_around_tip(&tip, 0).unwrap(), vec![]);
        for h in 0..=11 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![]
            );
        }
        for h in 12..=26 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![epoch2]
            );
        }
        for h in 27..=31 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![epoch3]
            );
        }
        for h in 32..40 {
            assert_eq!(
                epoch_manager
                    .possible_epochs_of_height_around_tip(&tip, genesis_height + h)
                    .unwrap(),
                vec![epoch3, epoch4]
            );
        }
    }
}

fn test_get_shard_uids_pending_resharding_base(shard_layouts: &[ShardLayout]) -> HashSet<ShardUId> {
    init_test_logger();

    // Create a minimal genesis.
    let mut genesis_config = GenesisConfig::default();
    genesis_config.protocol_version = PROTOCOL_VERSION;
    genesis_config.validators = vec![AccountInfo {
        account_id: "test".parse().unwrap(),
        public_key: PublicKey::empty(KeyType::ED25519),
        amount: 10,
    }];
    genesis_config.num_block_producer_seats = 1;
    genesis_config.num_chunk_producer_seats = 1;

    // Create an epoch config store with a new protocol version for each
    // provided shard layout.
    let mut epoch_config = EpochConfig::from(&genesis_config);
    let mut epoch_config_store = vec![];
    for (i, shard_layout) in shard_layouts.iter().enumerate() {
        let protocol_version = genesis_config.protocol_version + i as u32;
        epoch_config.shard_layout = shard_layout.clone();
        epoch_config_store.push((protocol_version, Arc::new(epoch_config.clone())));
    }
    let epoch_config_store = BTreeMap::from_iter(epoch_config_store.into_iter());
    let epoch_config_store = EpochConfigStore::test(epoch_config_store);

    // Create the epoch manager.
    let store = create_test_store();
    let epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
        store,
        &genesis_config,
        epoch_config_store,
    );

    // Get and return the ShardUIds pending resharding.
    let head_protocol_version = genesis_config.protocol_version;
    let client_protocol_version = genesis_config.protocol_version + shard_layouts.len() as u32 - 1;
    epoch_manager
        .get_shard_uids_pending_resharding(head_protocol_version, client_protocol_version)
        .unwrap()
}

/// Test there are no ShardUIds pending resharding when there are no planned
/// reshardings.
#[test]
fn test_get_shard_uids_pending_resharding_none() {
    let shard_layout = ShardLayout::single_shard();
    let shard_uids = test_get_shard_uids_pending_resharding_base(&[shard_layout]);
    assert_eq!(shard_uids.len(), 0);

    let shard_layout = ShardLayout::multi_shard(3, 3);
    let shard_uids = test_get_shard_uids_pending_resharding_base(&[shard_layout]);
    assert_eq!(shard_uids.len(), 0);

    let shard_layout = ShardLayout::multi_shard(3, 3);
    let shard_uids = test_get_shard_uids_pending_resharding_base(&[
        shard_layout.clone(),
        shard_layout.clone(),
        shard_layout,
    ]);
    assert_eq!(shard_uids.len(), 0);
}

/// Test there are no ShardUIds pending resharding when there are no planned
/// reshardings in the simple nightshade shard layout that is used in prod.
///
/// This test checks that when then protocol version is changing but the shard
/// layout is not, no shard is pending resharding.
#[test]
fn test_get_shard_uids_pending_resharding_simple_nightshade() {
    let epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let shard_layout = epoch_config_store.get_config(PROTOCOL_VERSION).shard_layout.clone();
    let shard_uids =
        test_get_shard_uids_pending_resharding_base(&[shard_layout.clone(), shard_layout]);
    assert_eq!(shard_uids.len(), 0);
}

/// Test that there is only one ShardUId pending resharding during a single
/// resharding.
#[test]
fn test_get_shard_uids_pending_resharding_single() {
    let version = 3;
    let a: AccountId = "aaa".parse().unwrap();
    let b: AccountId = "bbb".parse().unwrap();

    // start with just one boundary - a
    // the split s1 by adding b
    let shard_layout_0 = ShardLayout::multi_shard_custom(vec![a.clone()], version);
    let shard_layout_1 = ShardLayout::derive_shard_layout(&shard_layout_0, b);

    let s1 = shard_layout_0.account_id_to_shard_uid(&a);

    let shard_uids = test_get_shard_uids_pending_resharding_base(&[shard_layout_0, shard_layout_1]);
    assert_eq!(shard_uids, vec![s1].into_iter().collect::<HashSet<_>>());
}

/// Test that both original shards are pending resharding during a double
/// resharding of different shards.
#[test]
fn test_get_shard_uids_pending_resharding_double_different() {
    let version = 3;
    let a: AccountId = "aaa".parse().unwrap();
    let b: AccountId = "bbb".parse().unwrap();
    let c: AccountId = "ccc".parse().unwrap();

    // start with just one boundary - b
    // then split s0 by adding a
    // then split s1 by adding c
    // both original shards are pending resharding
    let shard_layout_0 = ShardLayout::multi_shard_custom(vec![b.clone()], version);
    let shard_layout_1 = ShardLayout::derive_shard_layout(&shard_layout_0, a.clone());
    let shard_layout_2 = ShardLayout::derive_shard_layout(&shard_layout_0, c);

    let s0 = shard_layout_0.account_id_to_shard_uid(&a);
    let s1 = shard_layout_0.account_id_to_shard_uid(&b);

    let shard_uids = test_get_shard_uids_pending_resharding_base(&[
        shard_layout_0,
        shard_layout_1,
        shard_layout_2,
    ]);
    assert_eq!(shard_uids, vec![s0, s1].into_iter().collect::<HashSet<_>>());
}

/// Test that only one shard is pending resharding during a double
/// resharding where the same shard is resharded twice.
#[test]
fn test_get_shard_uids_pending_resharding_double_same() {
    let version = 3;
    let a: AccountId = "aaa".parse().unwrap();
    let b: AccountId = "bbb".parse().unwrap();
    let c: AccountId = "ccc".parse().unwrap();

    // start with just one boundary - a
    // then split s1 by adding a
    // then split s1 by adding c
    // both original shards are pending resharding
    let shard_layout_0 = ShardLayout::multi_shard_custom(vec![a.clone()], version);
    let shard_layout_1 = ShardLayout::derive_shard_layout(&shard_layout_0, b);
    let shard_layout_2 = ShardLayout::derive_shard_layout(&shard_layout_0, c);

    let s1 = shard_layout_0.account_id_to_shard_uid(&a);

    let shard_uids = test_get_shard_uids_pending_resharding_base(&[
        shard_layout_0,
        shard_layout_1,
        shard_layout_2,
    ]);
    assert_eq!(shard_uids, vec![s1].into_iter().collect::<HashSet<_>>());
}
