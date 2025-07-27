const Records = require('./records.model');
const { vaciarCarpeta, splitCsvByLines, formatRecords, arrayOfPathFn } = require('./utils');

const upload = async (req, res) => {
    const {file} = req;
    const batchSize = 90000;
    
    if (!file)  return res.status(400).json({ message: 'No file uploaded' });

    try{
        const numFiles = await splitCsvByLines(file.path, batchSize, './_temp');
        const arrayOfPromises = arrayOfPathFn(numFiles, formatRecords);

        await Promise.all(arrayOfPromises);
        await vaciarCarpeta('./_temp');

        return res.status(200).json({ message: 'Archivo subido exitosamente', data: [] });
    } catch (err) {
        await vaciarCarpeta('./_temp'); 
        return res.status(500).json({ message: 'Error al procesar el archivo', error: err });
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
