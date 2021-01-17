import effects
from email.mime.text import MIMEText
import config


class EmailNotification:
    def send(self, to, content):
        conf = config.get('EMAIL_NOTIFICATION')
        with effects.Gmail(conf['account']) as mail:
            msg = MIMEText(content)
            msg['Content-Type'] = conf['content_type']
            msg['Subject'] = conf['title']
            msg['From'] = conf['account']
            msg['To'] = to
            mail.server.sendmail(conf['account'], to, msg.as_string())


class SMSNotification:
    def send(self, to, content):
        conf = config.get('SMS')
        with effects.SMS() as sms:
            sms.client.messages.create(
                     body=content,
                     from_=conf['from'],
                     to=to
                 )


if(__name__ == "__main__"):
    EmailNotification().send('zhiwilliam@gmail.com', "Hello")
    SMSNotification().send('+16475220400', "Hello")
