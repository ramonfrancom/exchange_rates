from airflow.models import BaseOperator
from airflow.utils.email import send_email

class SendMailOperator(BaseOperator):
    template_fields = ("distribution_list","subject","body")
    
    def __init__(self, distribution_list, subject, body, **kwargs):
        super.__init__(kwargs)
        self.distribution_list = distribution_list
        self.subject = subject
        self.body = body

    def execute(self, context):
        self.log.info('Sending Email to %s', self)
        send_email(self.distribution_list,self.subject,self.body)
        self.log.info('Email sent successfully')