import pika
import json
import base64


body = """Prezado(a), segue em anexo os dados.

 Por favor nao responda esse email.

 Por favor não marcar esse -mail como SPAM. Se deseja sair da lista envie e-mail para: meteo-ops@climatempo.com.br 

 Atenciosamente,

 CLIMATEMPO - A StormGeo Company
"""

def run(outputs, subject):
    configs = {
        "default":[
            {'email': 'lucas.marten@climatempo.com.br'},
        ],
    }

    emails = configs['default']

    rabbitmq_host = 'rabbitmq.climatempo.io'  # Host do servidor RabbitMQ
    rabbitmq_port = 5672                      # Porta do servidor RabbitMQ
    rabbitmq_user = 'rabman'                 # Nome de usuário
    rabbitmq_password = 'kld82wki'               # Senha
    exchange_name = 'delivery' # Nome da exchange onde a mensagem será enviada

    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)

    parameters = pika.ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        virtual_host='/',
        credentials=credentials
    )

    # Conectar ao RabbitMQ
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    encoded_contents = list()
    for output in outputs:

        with open(output, "rb") as file:
            encoded_content = base64.b64encode(file.read()).decode('utf-8')
            encoded_contents.append(encoded_content)

    attachments = [{
            'filename': outputs[i],
            'type':'application/zip',
            'encoding':'base64',
            'content': encoded_contents[i]
        } for i, outputs in enumerate(outputs)]
    
    print(attachments)
    
    message = {
        'sendType': 'SMTP_BCC',
        'contacts': emails,
        'subject': subject,
        'body': body,
        'isBodyHTML': False,
        'origin': "script",
        'attachments': attachments
    } 

    channel.basic_publish(
        exchange=exchange_name,
        routing_key='',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Tornar a mensagem persistente
        )
    )

    print(" [x] Enviado")

    connection.close()

    return True