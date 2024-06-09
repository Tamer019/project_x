import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def send_email(subject, body, to_email, from_email, smtp_server, smtp_port, login, password, attachment_path=None):
    # Erstellen der Nachricht
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    # Nachrichtentext hinzufügen
    msg.attach(MIMEText(body, 'plain'))

    # Anhang hinzufügen
    if attachment_path:
        attachment = open(attachment_path, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename= {attachment_path.split('/')[-1]}")
        msg.attach(part)

    # Verbindung zum Server aufbauen und E-Mail senden
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(login, password)
    text = msg.as_string()
    server.sendmail(from_email, to_email, text)
    server.quit()

# # Beispielhafte Verwendung
# send_email(
#     subject="Test E-Mail",
#     body="Dies ist eine Test-E-Mail mit Anhang.",
#     to_email="abdelrahmanta00@example.com",
#     from_email="abdelrahmanta00@gmail.com",
#     smtp_server="smtp.gmail.com",
#     smtp_port=587,
#     login="abdelrahmanta00@gmail.com",
#     password="qogz mjeo mrfn iwky",  # Verwenden Sie hier das generierte App-Passwort
#     attachment_path="C:/Users/Tamer/Desktop/project_x/project_x/FA/output_files/timediff_EVTEC_SuspendedEV_bis_Available.xlsx"
# )
