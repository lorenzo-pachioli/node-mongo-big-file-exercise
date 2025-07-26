const Records = require('./records.model');
const { vaciarCarpetaTemp } = require('./utils');
const fs = require('fs');
const csv = require('csv-parser');

const upload = async (req, res) => {
    const {file} = req;

    if (!file) {
        return res.status(400).json({ message: 'No file uploaded' });
    }

    const batchSize = 90000;
    let batch = [];
    let header = 0;

    try {
        const stream = fs.createReadStream(file.path)
            .pipe(csv({ headers: true }));

        stream.on('data', async (data) => {
            if( header == 0) { // Si es la primera fila, es el header del csv y lo ignoro
                header++;
            } else {
                // Les doy el formato del modelo a cada fila del csv
                const formatted = {
                    id: parseInt(data._0, 10) ? parseInt(data._0, 10) : 0,
                    firstname: data._1,
                    lastname: data._2,
                    email: data._3,
                    email2: data._4,
                    profession: data._5
                };

                // Agrego el objeto formateado al arreglo batch
                batch.push(formatted);

                // Si el batch alcanza el tamaño definido, lo inserto en la base de datos
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
            await vaciarCarpetaTemp('./_temp');            // Elimino el archivo temporal
            return res.status(200).json({ message: 'Archivo subido exitosamente', data: file });
        });

        stream.on('error', (err) => {
            return res.status(500).json({ message: 'Error al procesar archivo', error: err });
        });


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
