import paramiko
import boto3
import pandas as pd
from pandas import Series, DataFrame
import psycopg2
import json
from datetime import datetime
from datetime import timedelta
from io import StringIO, BytesIO # python3; python2: BytesIO
import time
import pytz
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText


def sendEmail (nameFile, fileCsv, credentials, text, booleanTransactions):
    try:
        
        # datos para el email, desde secret manager para mayor seguridad.
        server = credentials['emailServer']
        port = credentials['emailPort']
        sender = credentials['emailSender']
        senderPass = credentials['emailPass']
        body = text
        
        # creamos el objeto mensaje.
        message = MIMEMultipart("plain")
        
        if (booleanTransactions):
            receiver = credentials['emailReceiver'].split(',')
            subject = "KLAP - Error carga SFTP - Movimientos bancarios - Archivo " + nameFile
            
            # configuramos el archivo adjunto.
            attached = MIMEBase("application", "octect-stream")
            attached.add_header("content-Disposition",'attachment; filename=' + nameFile)
            attached.set_payload(fileCsv, 'utf-8')
            
            # agregamos el adjunto al mensaje.
            message.attach(attached)
        else:
            receiver = credentials['emailReceiverUs'].split(',')
            subject = "KLAP - Sin transacciones nuevas - Movimientos bancarios"
            
        
        # establecemos los atributos del mensaje.
        message['From'] = sender
        message['To'] = ", ".join(receiver)
        message['Subject'] = subject
        message.attach(MIMEText(body, 'plain')) #agregamos el cuerpo del mensaje como objeto MIME de tipo texto
        
        
        
        # conexion, autenticacion y envio de email.
        smtp = smtplib.SMTP(server, port) #.gmail o .yahoo
        smtp.ehlo() #tiene que ir antes para que el servidor entienda que eres un cliente "moderno" y te ofrezca la posibilidad de hacer STARTTLS. Y tiene que ir otra vez después para que el cliente descubra qué métodos de autenticación soporta ese servidor
        smtp.starttls()
        smtp.ehlo()
        smtp.login(sender, senderPass)
        smtp.sendmail(sender, receiver, message.as_string())
        smtp.quit()
        
        return "Email enviado satisfactoriamente!"
        
    except (Exception, psycopg2.Error) as error :
        
        return "Error al enviar el email: ", error




# definicion de funcion para cargar archivo al SFTP.
def uploadSftp (timeOut, retry, credentials, nameCsv, nameBucket, dataCsv, fileSize): # el tiempo de espera entre cada intento, y la cantidad de intentos.
    
    print("-----------------------------------------")

    # definimos los datos para conectar al SFTP, traidos del secret manager. (luego cambiar por los de klap)
    host = credentials['sftpKlapHost']
    port = credentials['sftpKlapPort']
    user = credentials['sftpKlapUser']
    password = credentials['sftpKlapPass']
    folder = credentials['sftpKlapFolder']
    attempts = 0 # la cantidad de intentos que lleva, 0 al empezar.
    nameFile = nameCsv # nombre archivo 
    locationServer = folder + nameCsv # ubicacion remota
    response = {
        "state": "Error",
        "msjEmail": "Estimados, Ha fallado el reintento de carga del archivo resultante del servicio automatizado de movimientos bancarios en el SFTP.  Por lo tanto el archivo va adjunto en este correo. Importante: Este correo ha sido generado de manera automática.  Por favor, no responda al mismo."
    }


    # hacemos este bucle para intentar las veces que vengan por parametro, la subida del archivo al sftp.
    while (attempts < retry):
        try:
            print("Intento de subir a SFTP n°: ", attempts+1)
            
            # configuramos el cliente
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy()) #seguridad
            client.connect(host, port, user, password) #hacemos la conexion al servidor
            print("Conexion creada")
            
            # creamos el canal sftp para subir archivo.
            sftp = client.open_sftp()
            print("Canal creado")
            
            # subimos el archivo.
            sftp.putfo(StringIO(dataCsv), locationServer, confirm = False) # parametro confirm, para que al subir el archivo, no se haga una estadistica del tamaño, asi no retorna un warning y sale por la excepcion.
            print("Archivo subido")
            
            try:
                print("Chequeando que se haya subido correctamente.")
                fileInfo = sftp.stat(locationServer)
                print("Tamaño de archivo subido al SFTP: ", fileInfo.st_size)
                if (fileSize == fileInfo.st_size):
                    response["state"] = "Bien"
                    print("Subido correctamente!")
                    break
                else:
                    print("Por alguna razon, el archivo se subio corrupto o no coincide con el original.")
                    attempts += 1
                    if (attempts < retry):
                        print(f'Esperando por {timeOut} segundos')
                        time.sleep(timeOut)
                    else:
                        #borrar archivo malo del sftp
                        response["state"] = "Error"
                        response["msjEmail"] = 'Estimados, Ha fallado el reintento de carga del archivo resultante del servicio automatizado de movimientos bancarios en el SFTP.  Por lo tanto, el archivo va adjunto en este correo. Importante: Este correo ha sido generado de manera automática.  Por favor, no responda al mismo.'
                        break
            except FileNotFoundError:
                print("El archivo aparentemente se subio bien, pero al ir a buscarlo al servidor SFTP, no se encontró.")
                attempts += 1
                if (attempts < retry):
                    print(f'Esperando por {timeOut} segundos')
                    time.sleep(timeOut)
                else:
                    response["state"] = "Error"
                    response["msjEmail"] = 'Estimados, Ha fallado el reintento de carga del archivo resultante del servicio automatizado de movimientos bancarios en el SFTP.  Por lo tanto, el archivo va adjunto en este correo. Importante: Este correo ha sido generado de manera automática.  Por favor, no responda al mismo.'
                    break
            
            
            # cerramos la conexion
            client.close()
            
        except (Exception, psycopg2.Error) as error :
            print("Error: ", error)
            print(f'Esperando por {timeOut} segundos')
            time.sleep(timeOut)
            attempts += 1
        
        print("------------------")
        

    return response





# definicion de funcion principal.
def createFile (event, context):
    
    print("******''''******''''******''''******''''******''''******''''******''''******''''******''''******''''******''''******''''******''''******''''")
    
    # definicion de variables.
    bank = event['bank']
    response = ""
    cltime = pytz.timezone('America/Santiago')
    dateLocal = datetime.now(cltime)
    dateFormat = dateLocal.strftime('%Y%m%d%H%M%S')
    bucketQa = "199-klap-movimientosbancarios"

    
    # FROM y WHERE de la consulta, para no repetir codigo en todas las consultas.
    aliasQuery = "FROM random.banco AS banco, random.cuenta AS cuenta, random.nomina AS nomina "
    filterQuery = "WHERE banco.id = cuenta.banco_id AND cuenta.cuenta = nomina.cuenta_id AND nomina.estado_id = 0 AND banco.nombre = '" + bank + "' AND nomina.abono <> 0"
    
    # consulta para UPDATE, igual para cualquier banco.
    queryUpd = "SELECT nomina.id " + aliasQuery + filterQuery


    # datos para Secrets Manager
    client = boto3.client('secretsmanager') #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/secretsmanager.html
    secretName = "secret/credentials"

    # creamos el cliente de Secrets Manager.
    credentials = client.get_secret_value(
        SecretId = secretName
    )
        
    # en este diccionario guardamos las keys de Secrets Manager.
    secretDict = json.loads( credentials['SecretString'] ) 


    # condiciones para elegir los datos dependiendo el banco del parametro.
    if ( bank == "BCI" ):
        queryGet = "SELECT nomina.id, nomina.fecha_movimiento, nomina.descripcion, nomina.n_documento, nomina.abono, nomina.cargo " + aliasQuery + filterQuery + ";" 
        columns = ['Fecha','Descripcion','Serial Documento','Cargo ($)', 'Abono ($)'] 
        countBank = secretDict['countBCI']
        nameCsv = bank + "_" + countBank + "_" + dateFormat + ".csv"
        
        
    elif ( bank == "CHILE" ):
        queryGet = "SELECT nomina.id, nomina.fecha_movimiento, nomina.descripcion, nomina.n_documento, nomina.abono, nomina.cargo " + aliasQuery + filterQuery + ";"
        columns = ['Fecha','Descripcion','Serial Documento','Cargo ($)', 'Abono ($)'] 
        countBank = secretDict['countCHILE']
        nameCsv = f"BANCODE{bank}_00{countBank}_{dateFormat}.csv"
        
      
    elif ( bank == "SANTANDER" ):
        queryGet = "SELECT nomina.id, to_char(to_date(nomina.fecha_movimiento, 'DD-MM-YYY'), 'DD-mon'), nomina.sucursal, nomina.descripcion, nomina.n_documento, 'A', nomina.abono, nomina.saldo " + aliasQuery + filterQuery + ";" 
        columns = ['Fecha','Sucursal','Detalle de Transaccion','N Dcto','Cheques y Otros Cargos','Depositos y Otros Abonos','Saldo'] 
        countBank = secretDict['countSANTANDER']
        nameCsv = bank + "_" + countBank + "_" + dateFormat + ".csv"
        
    elif ( bank == "ESTADO" ):
        queryGet = "SELECT nomina.id, to_char(to_date(nomina.fecha_movimiento, 'DD-MM-YYY'), 'DD/MM'), 'STGO.PRINCIPAL', nomina.n_documento, nomina.descripcion, nomina.cargo, nomina.abono, nomina.saldo " + aliasQuery + filterQuery + ";"
        columns = ['Fecha','Sucursal','N operacion','Descripcion','Cheques/Cargos($)','Depositos/Abonos($)','Saldo$'] 
        countBank = secretDict['countESTADO']
        nameCsv = bank + "_" + countBank + "_" + dateFormat + ".csv"
       
    else:
        response = "¡El nombre del banco fue mal ingresado!"
        return {
            'statusCode': 200,
            'body': response
        } 



    # armamos el nombre del archivo CSV segun los datos del banco.
    print("Nombre Archivo: ", nameCsv)  
    print("Query Get: ", queryGet)
    
    
    try:
        print("Inicio del proceso")
        
        
        # hacemos la conexion a la base de datos, usando las credenciales obtenidas de Aws.
        connection = psycopg2.connect(
            host =     secretDict['host'],
            password = secretDict['password'],
            database = secretDict['database'],
            port =     secretDict['port'],
            user =     secretDict['user'],
        )
        cursor = connection.cursor()
        
        
        # ejecutamos la consulta.
        cursor.execute(queryGet) 
        connection.commit()
        print("Query ejecutada")
        
        
        
        # tomamos el valor obtenido, un array con todos los registros.
        resultQuery = cursor.fetchall()
        itemsList = []
        idList = []
        
        # valido si vinieron transacciones nuevas en la consulta. si no hay ninguna, se corta el proceso. lo hago antes del IF para no hacerlo innecesariamente.
        if ( len(resultQuery) == 0 ):
            response = "¡No hay transacciones nuevas para el banco '"+bank+"'!"
            
            # enviar correo notificando que no hay transacciones nuevas. (puede ser porque simplemente no hay, o no trajo porque en el otro lambda tuvo error de login o error con el banco)
            # print("Se notificara al equipo, para revisar en caso de que exista algun error de login o con el banco.")
            # msjCorreo = f"Estimados, no hay transacciones nuevas en la base de datos: Banco '{bank}'   -   Fecha '{dateFormat}'."
            # responseEmail = sendEmail("", "", secretDict, msjCorreo, False)
            # print("Envio de EMAIL: ", responseEmail)
           
            return {
                'statusCode': 200,
                'body': response
            } 
        
        print("Transacciones traidas de la base de datos: ", resultQuery)
        print("Cantidad transacciones: ", len(resultQuery))
        
        # armamos una lista con listas(cada registro/movbancario) para luego armar el DF
        if ( bank == "BCI" or bank == "CHILE" ):
            for i in resultQuery:
                item = [
                    i[1],
                    i[2],
                    i[3],
                    i[4],
                    i[5],
                ]
                
                idList.append(i[0])
                itemsList.append(item)
                
        elif ( bank == "SANTANDER" or bank == "ESTADO" ):
            for i in resultQuery:
                item = [
                    i[1],
                    i[2],
                    i[3],
                    i[4],
                    i[5],
                    i[6],
                    i[7]
                ]
                
                idList.append(i[0])
                itemsList.append(item)
                
        
        print("Lista ID's: ", idList)
        print("Cantidad ID's: ", len(idList))  
        
     
        
        # creamos dataframe a partir del array creado con los registros pendientes y las columnas.
        dfList = pd.DataFrame(data = itemsList, columns = columns)
        print("Data frame: \n", dfList)
        
        
        # variable donde nos queda guardado el csv.
        bufferCsv = StringIO()
        

        # creamos el archivo csv.
        dfList.to_csv(bufferCsv, index = False) 
        print("Csv: ", bufferCsv.getvalue())
     
        
        # enviamos el CSV al S3
        s3 = boto3.resource('s3')
        s3.Object(bucketQa, nameCsv).put(Body = bufferCsv.getvalue())
        print("Carga al S3 correctamente")
        
        
        # captura de tamaño del archivo subido
        s3Client = boto3.client('s3')
        captureSize = s3Client.head_object(Bucket=bucketQa, Key=nameCsv)
        fileSize = captureSize['ContentLength']
        print("Tamaño de archivo subido al S3: ", fileSize)
        

        # enviamos el CSV al SFTP
        responseSftp = uploadSftp(5, 4, secretDict, nameCsv, bucketQa, bufferCsv.getvalue(), fileSize) # 10 segundos y 4 intentos.
        print("Carga al SFTP: ", responseSftp)
        
        
        # envio de correo
        if (responseSftp["state"] == "Error"):
            # llamo funcion que envia el email.
            print("Se notificara del error al cliente por correo, adjuntando el archivo.")
            responseEmail = sendEmail(nameCsv, bufferCsv.getvalue(), secretDict, responseSftp["msjEmail"], True)
            print("Envio de EMAIL: ", responseEmail)
        
        
        # update de registros, cambiar estado a 1 y nombre del archivo.
        if (len(idList) > 1):
            queryUpdate = f"UPDATE random.nomina SET estado_id = 1, updated_at = '{str(dateLocal)}', nombre_archivo = '{nameCsv}' WHERE id IN {tuple(idList)};" #str(fecha) para concatenar una fecha tipo date con string, y la lista de ID's va en tupla
        else:
            queryUpdate = f"UPDATE random.nomina SET estado_id = 1, updated_at = '{str(dateLocal)}', nombre_archivo = '{nameCsv}' WHERE id = '{idList[0]}';" #str(fecha) para concatenar una fecha tipo date con string, y la lista de ID's va en tupla

        
        print("Query Update: ", queryUpdate)
        cursor.execute(queryUpdate)
        print("Ejecutada con exito!")
        connection.commit()
        
        
        # agregamos un registro a la tabla 'cruce_archivos_transacciones' para luego cruzar los datos con los que hay en el SFTP y ver cuando haya una falla de faltar una transaccion
        queryInsert = f"insert into random.cruce_archivos_transacciones (nombre_archivo, cantidad_registros, created_at) VALUES ('{nameCsv}', {len(resultQuery)}, '{str(dateLocal)}');" 
        print("Query Insert (guardamos el nombre del archivo y la cantidad de transacciones para luego cruzar los datos): ", queryInsert)
        cursor.execute(queryInsert)
        print("Ejecutada con exito!")
        connection.commit()
        
        cursor.close()
        connection.close()
        
        
        # mensaje satisfactorio
        response = "¡Todo correcto!"
        
        
    except (Exception, psycopg2.Error) as error :
        response = error
        
        
    except ValueError:
        response = "Banco a buscar no definido"
        
        
    finally:
        print(response)


    return {
        'statusCode': 200,
        'body': response
    }