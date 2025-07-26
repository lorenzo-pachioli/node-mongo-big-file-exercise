const fs = require('fs').promises;
const path = require('path');

const vaciarCarpetaTemp = async (directorio) => {
    const archivos = await fs.readdir(directorio);
    for (const archivo of archivos) {
        const ruta = path.join(directorio, archivo);
        await fs.unlink(ruta);
    }
}

module.exports = {
    vaciarCarpetaTemp
};