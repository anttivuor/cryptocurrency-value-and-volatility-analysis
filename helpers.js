const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const moment = require('moment');
const {getTableHeaders, dataKeys} = require('./constants');

const monthToNumber = (monthString) => {
    const months = {
        Jan: '01',
        Feb: '02',
        Mar: '03',
        Apr: '04',
        May: '05',
        Jun: '06',
        Jul: '07',
        Aug: '08',
        Sep: '09',
        Oct: '10',
        Nov: '11',
        Dec: '12',
    };
    return months[monthString];
};

const formatDate = (date) => {
    const [month, day = '', year] = date.split(' ');
    return `${year}-${monthToNumber(month)}-${day.replace(',', '')}`;
};

const formatDateMonthly = (date) => {
    const [month, year] = date.split(' ');
    return `20${year}-${monthToNumber(month)}`
}

const readFile = async (filename, type = 'daily') => {
    return new Promise((resolve, reject) => {
        const results = [];
        const headers = getTableHeaders(filename);
        return fs.createReadStream(`./data/${type}/${filename}.csv`)
            .pipe(csv({
                headers,
            }))
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results));
    });
};

const getVolatilityForEachYear = (data) => {
    const years = [];
    console.log('CALCULATING VOLATILITY');
    const length = data.length;
    console.log('length', length);
    data.forEach((row, index) => {
        const percentage = Math.round((index / length) * 100) ;
        if (index % 15 === 0) console.log(`Processing... (${percentage}%) (${row.date})`);
        dataKeys.forEach((key) => {
            let prevDay;
            for (let i = 1; i < 7 && prevDay === undefined; i++) {
                prevDay = data.find((p) => moment(row.date).subtract(i, 'd').isSame(moment(p.date)) && !!p[`price_${key}`]);
            }
            if (!prevDay) return;
            // else console.log(`${key}:`, prevDay.date);
            if (row[`price_${key}`] !== undefined && prevDay !== undefined) {
                const year = row.date.split('-')[0];
                const findIndex = years.findIndex(y => y.year === year);
                const continousReturn = Math.log(row[`price_${key}`] / prevDay[`price_${key}`]);
                if (findIndex === -1) {
                    years.push({
                        year,
                        [`values_${key}`]: [continousReturn],
                        [`count_${key}`]: 1,
                    });
                } else {
                    years[findIndex] = {
                        ...years[findIndex],
                        [`values_${key}`]: [...(years[findIndex][`values_${key}`] || []), continousReturn],
                        [`count_${key}`]: (years[findIndex][`count_${key}`] || 0) + 1,
                    };
                }
            }
        });
    });

    const volatilityForEachYear = [];

    years.forEach((year) => {
        const res = {year: year.year};
        dataKeys.forEach((key) => {
            if (!year[`values_${key}`]) return;
            const continousReturnSum = year[`values_${key}`].reduce((p, c) => p + c, 0);
            const meanPriceChange = continousReturnSum / year[`count_${key}`];
            const deviations = year[`values_${key}`].map((val) => val - meanPriceChange);
            const deviationsSquared = deviations.map((d) => Math.pow(d, 2))
            const deviationsSquaredSum = deviationsSquared.reduce((p, c) => p + c, 0);
            const variance = deviationsSquaredSum / (year[`count_${key}`] - 1);
            const standardDeviation = Math.sqrt(variance);
            const annualizedHistoricalVolatility = standardDeviation * Math.sqrt(year[`count_${key}`]);

            res[`sd_${key}`] = standardDeviation;
            res[`annualized_hv_${key}`] = annualizedHistoricalVolatility;
        })
        volatilityForEachYear.push(res);
    });

    return volatilityForEachYear;
};

const scalePriceBasedOnTheFirstPrice = async (data) => {
    const firstPrices = {};
    const res = data.map((row) => {
        dataKeys.forEach((key) => {
            if (row[`price_${key}`] !== 0) {
                if (firstPrices[key] !== undefined) {
                    row[`price_${key}`] = (row[`price_${key}`] / firstPrices[key]) - 1;
                } else {
                    firstPrices[key] = row[`price_${key}`];
                    row[`price_${key}`] = 0;
                }
            }
        })
        return row;
    })
    return res;
}

const writeCsv = (data, name = 'volatilities') => {
    const headers = Object.keys(data[0]);
    const csvWriter = createCsvWriter({
        path: `${name}.csv`,
        header: headers.map((h) => ({id: h, title: h})),
    });

    csvWriter
        .writeRecords(data)
        .then(() => console.log(`Data was processed. Output in file ${name}.csv`));
}

module.exports = {
    formatDate,
    formatDateMonthly,
    readFile,
    getVolatilityForEachYear,
    scalePriceBasedOnTheFirstPrice,
    writeCsv,
};