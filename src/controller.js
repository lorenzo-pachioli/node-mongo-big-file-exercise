const Records = require('./records.model');
const fs = require('fs');
const csv = require('csv-parser');

const upload = async (req, res) => {
    const {file} = req;

    if (!file) {
        return res.status(400).json({ message: 'No file uploaded' });
    }

    const batchSize = 1000;
    let batch = [];
    let header = 0;

    try {
       
        const stream = fs.createReadStream(file.path)
            .pipe(csv({ headers: true }));

        stream.on('data', async (data) => {
            if( header == 0) {
                header++;
            } else {
            const formatted = {
                id: parseInt(data._0, 10) ? parseInt(data._0, 10) : 0,
                firstname: data._1,
                lastname: data._2,
                email: data._3,
                email2: data._4,
                profession: data._5
            };

            batch.push(formatted);
            if (batch.length >= batchSize) {
                stream.pause();
                await Records.insertMany(batch);
                batch = [];
                stream.resume();
            }
            }
        });
        header = 0;
        stream.on('end', async () => {
            if (batch.length > 0) {
                await Records.insertMany(batch);
            }
            return res.status(200).json({ message: 'Archivo procesado y subido exitosamente' });
        });

        stream.on('error', (err) => {
            return res.status(500).json({ message: 'Error al procesar archivo', error: err });
        });

       
         return res.status(200).json({ message: 'Archivo subido exitosamente', data: file });
    } catch (err) {
        return res.status(500).json(err);
    }
};

const list = async (_, res) => {
    try {
        const data = await Records
            .find({})
            .limit(10)
            .lean();
        
        return res.status(200).json(data);
    } catch (err) {
        return res.status(500).json(err);
    }
};

module.exports = {
    upload,
    list,
};
