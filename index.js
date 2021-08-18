const moment = require('moment');
const {dataKeys} = require('./constants');
const {
    formatDate,
    readFile,
    getVolatilityForEachYear,
    writeCsv,
} = require('./helpers');

const data = async () => {
    return new Promise((resolve, reject) => {
        const result = {};
        let ready = 0;
        dataKeys.forEach(async (key) => {
            console.log(key);
            const results = await readFile(key);
            const formattedResults = results.map((res) => {
                res.date = formatDate(res.date);
                return res;
            });
            result[key] = formattedResults;
            ready += 1;
            if (ready === dataKeys.length) resolve(result);
        });
    });
}

const combineData = async (data) => {
    return new Promise((resolve, reject) => {
        const array = [];
        console.log(Object.keys(data));
        dataKeys.forEach((key, index) => {
            console.log(`PROCESSING: ${key} (${index + 1}/${dataKeys.length})`);
            data[key].forEach((row, i) => {
                if (i === 0) return;
                const findIndex = array.findIndex((x) => moment(x.date).isSame(moment(row.date)));
                const dayOfTheYear = moment(row.date).dayOfYear();
                if (dayOfTheYear % 15 === 0) {
                    const years = 2021 - moment(row.date).year();
                    const total = 11 * 365; // Only rough estimate --> percentages are not exactly correct
                    const days = 365 - dayOfTheYear;
                    const percentage = Math.round(((years * 365 + days) / total) * 100);
                    console.log(moment(row.date).format(`[${key}:] YYYY [(${percentage}%)]`));
                }
                const obj = {
                    date: row.date,
                    [`price_${key}`]: Number(row.price.replace(/,/g, '')),
                }
                if (key === 'jpy') obj.price_jpy = 1 / obj.price_jpy;
                if (findIndex === -1) {
                    array.push(obj);
                } else {
                    array[findIndex] = {...array[findIndex], ...obj};
                }
            })
            if (index === dataKeys.length - 1) resolve(array);
        })
    });
};

const sortData = (data) => {
    console.log('SORTING LIST');
    const sorted = data.sort((a, b) => (
        moment(a.date).unix() - moment(b.date).unix()
    ));
    console.log('SORTING DONE');
    return sorted;
}

const calculateVolatility = async (data) => {
    return new Promise((resolve, reject) => {
        const sd = getVolatilityForEachYear(data);
        return resolve(sd);
    });
};

data()
.then((res) => {
    combineData(res)
    .then((res) => {
        const sorted = sortData(res);
        calculateVolatility(sorted)
        .then((res) => {
            console.log('RESULTS:');
            console.log(res);
            writeCsv(res);
        });
    })
});