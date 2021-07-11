const MinerMethods = {
    Constructor: 1,
    ControlAddresses: 2,
    ChangeWorkerAddress: 3,
    ChangePeerID: 4,
    SubmitWindowedPoSt: 5,
    PreCommitSector: 6,
    ProveCommitSector: 7,
    ExtendSectorExpiration: 8,
    TerminateSectors: 9,
    DeclareFaults: 10,
    DeclareFaultsRecovered: 11,
    OnDeferredCronEvent: 12,
    CheckSectorProven: 13,
    ApplyRewards: 14,
    ReportConsensusFault: 15,
    WithdrawBalance: 16,
    ConfirmSectorProofsValid: 17,
    ChangeMultiaddrs: 18,
    CompactPartitions: 19,
    CompactSectorNumbers: 20,
    ConfirmUpdateWorkerKey: 21,
    RepayDebt: 22,
    ChangeOwnerAddress: 23,
};

module.exports = {
    MinerMethods
};