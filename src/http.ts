const { createServer } = require('http');

function startHttpServer(port: number) {
  return new Promise((resolve, reject) => {
    const server = createServer();

    server.on('error', (err: any) => {
      reject(err);
    });

    server.listen(port, () => {
      console.log('Server listening on port', port);
      resolve(port);
    });
  });
}

startHttpServer(4009);
