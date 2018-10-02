const CSVParse = require('csv-parse');
const FS = require('fs');

const differentReports = {};
const output = FS.createWriteStream('output.csv', { flags: 'as' });
//const output = FS.createWriteStream('noAMIDMapping.log', { flags: 'as' });

const parser = CSVParse({ columns: true }, (err, mappings) => {
    let pointer = 0;
    const fundCombos = {}

    const nextOne = () => {
        if (pointer < mappings.length - 1) return cb(mappings[pointer++]);

        Object.keys(fundCombos).forEach((key, index) => {
            const group = fundCombos[key];
            output.write(`Group ${index + 1}\n`);
            output.write(`${["Funds"].concat(group.funds).join()}\n`);
            output.write(`${["Emails"].concat(group.emails).join()}\n`);
            output.write(`${["Entities"].concat(group.entities).join()}\n`);
            output.write('\n')
        });

        return true;
    };

    const cb = (mapping) => {
        const funds = [];
        const email = [];
        const entities = [];

        Object.keys(mapping)
            .filter((key) => !["Last Name", "Entity", "Email Address", "Notes"].includes(key.trim()))
            .forEach((fund) => {
                const cleaned_fund = fund.trim();
                if (mapping[fund]) {
                    funds.push(cleaned_fund);
                }
            });

        const key = funds.join("--");
        if (fundCombos[key]) {
            fundCombos[key].emails.push(mapping["Email Address"]);
            fundCombos[key].entities.push(mapping["Entity"]);
        }
        else {
            fundCombos[key] = {
                funds: funds,
                emails: [mapping["Email"]],
                entities: [mapping["Entity"]]
            };
        }

        nextOne();
    };

    cb(mappings[0]);
});

FS.createReadStream('./monthly_list.csv').pipe(parser);
