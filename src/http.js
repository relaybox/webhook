const { createServer } = require('http');

function startHttpServer(port) {
  return new Promise((resolve, reject) => {
    const server = createServer();

    server.on('error', (err) => {
      reject(err);
    });

    server.listen(port, () => {
      console.log('Server listening on port', port);
      resolve();
    });
  });
}

startHttpServer(4009);
