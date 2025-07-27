const fs = require("node:fs");
const fsPromise = require("node:fs").promises;
const path = require("node:path");
const csv = require("csv-parser");
const Records = require("./records.model");

const vaciarCarpeta = async (directorio) => {
  const archivos = await fsPromise.readdir(directorio); //crea un array con los nombres de los archivos en el directorio
  
  for (const archivo of archivos) { 
    const ruta = path.join(directorio, archivo);
    await fsPromise.unlink(ruta); // Elimina cada archivo en el directorio
  }
};

async function splitCsvByLines(filePath, batchSize, tempDir) {
  return new Promise((resolve, reject) => {

    if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });

    const readStream = fs.createReadStream(filePath, { encoding: "utf-8" });
    let fileIndex = 0;
    let lineCount = 0;
    let writeStream;
    let leftover = "";
    let header = null;
    let isFirstLine = true;

    readStream.on("data", (chunk) => {
      let lines = (leftover + chunk).split("\n");
      leftover = lines.pop(); // Guarda la última línea para el siguiente chunk

      for (let line of lines) {

        if (isFirstLine) {
          header = line;
          // Primer archivo, escribe encabezado
          writeStream = fs.createWriteStream(path.join(tempDir, `part_${fileIndex}.csv`));
          writeStream.write(header + "\n");
          isFirstLine = false;
          continue;
        }

        if (lineCount === 0 && fileIndex > 0) {
          // Nuevo archivo, escribe encabezado
          writeStream = fs.createWriteStream(path.join(tempDir, `part_${fileIndex}.csv`));
          writeStream.write(header + "\n");
        }

        // Agrega la línea al archivo actual
        writeStream.write(line + "\n");
        lineCount++;

        if (lineCount >= batchSize) {
          // Cierra el archivo actual y comienza uno nuevo
          writeStream.end();
          fileIndex++;
          lineCount = 0;
        }
      }
    });

    readStream.on("end", async () => {

      if (leftover) writeStream.write(leftover + "\n"); // Si hay una línea sobrante, la escribe
      writeStream.end();
      await fsPromise.unlink(filePath); // Elimina el archivo original luego de procesarlo
      resolve(fileIndex + 1); // Número de archivos generados
    });

    readStream.on("error", reject);
  });
}

const formatRecords = async (path) => {

  let batch = [];
  let header = 0;

  return new Promise((resolve, reject) => { // creo una promesa para manejar el flujo de datos del csv de manera asincrona
    try {
      const stream = fs.createReadStream(path).pipe(csv({ headers: true }));

      stream.on("data", async (data) => {
        if (header == 0) {
          // Si es la primera fila, es el header del csv y lo ignoro
          header++;
        } else {
          // Les doy el formato del modelo a cada fila del csv
          const formatted = new Records({
            id: parseInt(data._0, 10) ? parseInt(data._0, 10) : 0,
            firstname: data._1,
            lastname: data._2,
            email: data._3,
            email2: data._4,
            profession: data._5,
          });

          // Agrego el objeto formateado al arreglo batch
          batch.push(formatted);
        }
      });

      header = 0;
      stream.on("end", async () => {
        await Records.insertMany(batch); 
        batch = []; // Limpio el batch para liberar memoria
        return resolve(batch);
      });

      stream.on("error", (err) => {
        return reject({ message: "Error al procesar archivo", error: err });
      });
    } catch (err) {
      return reject(err);
    }
  });
};

const arrayOfPathFn = (numFiles, fn) => {
  const functions = [];

  // Genera un array de funciones que toman la ruta del archivo como argumento
  for (let i = 0; i < numFiles; i++) {
    const filePath = `./_temp/part_${i}.csv`;
    functions.push(fn(filePath));
  }
  return functions;
};

module.exports = {
  vaciarCarpeta,
  splitCsvByLines,
  arrayOfPathFn,
  formatRecords,
};
