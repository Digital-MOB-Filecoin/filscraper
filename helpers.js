const FILECOIN_GENESIS_UNIX_EPOCH = 1598306400;

const heightToUnix = (filEpoch) => {
    return (filEpoch * 30) + FILECOIN_GENESIS_UNIX_EPOCH;
}

const unixToHeight = (unixEpoch) => {
    return Math.floor((unixEpoch - FILECOIN_GENESIS_UNIX_EPOCH) / 30);
}

module.exports = {
    FILECOIN_GENESIS_UNIX_EPOCH,
    heightToUnix,
    unixToHeight,
}