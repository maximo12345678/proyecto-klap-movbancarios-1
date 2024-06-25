Este lambda desarrollado en python, tenia un Event Bridge que lo ejecutaba cada 5 minutos, segun que banco (santander, bci) iba a buscar a 
la base de datos las nuevas transacciones de ese banco. El proceso las trae, les da cierto formato, arma un archivo 
csv con la informacion, se envia a guardar en un S3, se envia a guardar a un SFTP del cliente. Y para los casos en los que
el sftp falle por alguna razon, se envia por correo el archivo adjunto. 

Todos los datos sensibles son capturados por el servicio de Aws Secret Manager.
