function hdiff(timestamp) {
    return (Math.abs(Date.now() - timestamp) / (1000 * 3600)).toFixed();
}

function ddiff(timestamp) {
    return (Math.abs(Date.now() - timestamp) / (1000 * 3600 * 24)).toFixed();
}

module.exports = {
    hdiff,
    ddiff
};