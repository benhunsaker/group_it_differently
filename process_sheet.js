const CSVParse = require('csv-parse');
const FS = require('fs');

const sampleZeroUsage = require('./sampleZeroUsage');

const mappedZeroUsageID = [];
const zeroUsageMappingStream = FS.createWriteStream('zeroUsageMapping.csv', { flags: 'as' });
const noAMIDMappingLog = FS.createWriteStream('noAMIDMapping.log', { flags: 'as' });
zeroUsageMappingStream.write('"BMX_BSS_CUSTID","AMID"\n');

const parser = CSVParse({ columns: true }, (err, mappings) => {
    let pointer = 0;

    const nextOne = () => {
        if (pointer < mappings.length - 1) return cb(mappings[pointer++]);

        sampleZeroUsage.forEach((zeroUsageId) => {
            if (!mappedZeroUsageID.includes(zeroUsageId)) noAMIDMappingLog.write(`${zeroUsageId}\n`);
        });
        noAMIDMappingLog.end();
        zeroUsageMappingStream.end();

        return true;
    };

    const cb = (mapping) => {
        console.log(`INFO:\t Processing mapping ${pointer + 1} of ${mappings.length}`);

        mapping.IDs.split('|').map((id) => {
            const idSplit = id.split(':');
            const valueSplit = idSplit[1].split(',');

            if (idSplit[0] === 'BMX_BSS_CUSTID') {
                valueSplit.forEach((BMX_BSS_CUSTID) => {
                    if (sampleZeroUsage.includes(BMX_BSS_CUSTID)) {
                        console.log(`INFO:\t Found a mapping`);
                        mappedZeroUsageID.push(BMX_BSS_CUSTID);
                        zeroUsageMappingStream.write(`"${BMX_BSS_CUSTID}","${mapping.AMID}"\n`);
                    }
                });
            }
        });

        nextOne();
    };

    cb(mappings[0]);
});

FS.createReadStream('./w_wdc_entitlement.csv').pipe(parser);
