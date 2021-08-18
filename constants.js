const dataKeys = [
    'bitcoin',
    'ethereum',
    'litecoin',
    'xrp',
    'eur',
    'gbp',
    'jpy',
    'gold',
    'silver',
    'brent_oil',
    'wti_oil',
];

const tableHeaders = {
    currency: ['date', 'price', 'open', 'high', 'low', 'change'],
    other: ['date', 'price', 'open', 'high', 'low', 'vol', 'change'],
}

const getTableHeaders = (key) => {
    switch (key) {
        case 'eur':
        case 'gbp':
        case 'jpy':
            return tableHeaders.currency;

        default:
            return tableHeaders.other;
    }
}

module.exports = {
    dataKeys,
    getTableHeaders,
};