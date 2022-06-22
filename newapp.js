import { BlobServiceClient } from '@azure/storage-blob';
import openpgp from 'openpgp';
import fs, { ReadStream } from 'fs';
import * as readline from 'node:readline';
import  { Writable } from 'stream';
import { chunk } from 'chunk';
import { Kafka } from 'kafkajs';
//const { Writable } = require('node:stream');

// import { Kafka } from 'kafkajs';

// openpgp.config_allow_unauthenticated_stream = true;

(async() => {

    const kafka = new Kafka({
        clientId: "sam",
        brokers: ["127.0.0.1:9092"],
    });

    const AZURE_STORAGE_CONNECTION_STRING = 'AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;'

    const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);

    const containerName = 'files';
    const containerClient = blobServiceClient.getContainerClient(containerName);

    const blobName = 'gap_uat_profile_extract_2022042001000203.csv.gpg';
    const blobClient = containerClient.getBlockBlobClient(blobName);

    const encryptedDataStream = (await blobClient.download(0)).readableStreamBody;
    encryptedDataStream.setEncoding('utf-8');


    const passphrase = 'Loy@ltymtlaccountmatch';
    const privateKeyArmored = fs.readFileSync('PrivateKey.asc', 'utf-8');
    
    

    const privateKey = await openpgp.decryptKey({
        privateKey: await openpgp.readPrivateKey({armoredKey: privateKeyArmored}),
        passphrase
    });
    
    const decryptedData = await openpgp.decrypt({
        message: await openpgp.readMessage({armoredMessage: encryptedDataStream}),
        decryptionKeys: privateKey,
        config: {allowUnauthenticatedStream: true}
    });

  

    
   // const jerseyNumber = process.argv[2];

   const producer = kafka.producer();
   await producer.connect();
   console.log("Producer connected");

   
   
   
    // const outStream = fs.createWriteStream('DecryptedFile.csv', 'utf-8');

    // decryptedData.data.pipe(outStream);
    const outStream = new Writable();
    outStream.write = function (chunk,encoding)  {
        //console.log(chunk.toString());
        const temp = chunk.toString();
        

          producer.send({
            topic: 'samirrrr',
            messages: [
              { value: temp,
               partition:  0,
                   },
            ],
          })
          console.log("sameeeeeeeeeeeeeeeeeeeer");
          

      };
      decryptedData.data.pipe(outStream);

      // await producer.disconnect()
})();





