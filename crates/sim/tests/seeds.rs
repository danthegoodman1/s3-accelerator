use s3_accelerator_sim::Simulator;

#[test]
fn seeds_pass() {
    for seed in 0..200 {
        if let Err(failure) = Simulator::from_seed(seed).run() {
            panic!("{failure}");
        }
    }
}

#[test]
fn a_seed_replays_exactly() {
    for seed in [1, 42, 0xdead_beef] {
        let first = Simulator::from_seed(seed).run().unwrap();
        let second = Simulator::from_seed(seed).run().unwrap();
        assert_eq!(first, second);
    }
}
