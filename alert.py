import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_alert_email(bad_weather, logger):
    # list of email_id to send the mail
    li = ["***@outlook.com", "***@gmail.com"]

    for dest in li:
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login("***@gmail.com", "***")

        html_content = """
          <!DOCTYPE html>
          <html lang="en">
          <head>
              <meta charset="UTF-8">
              <title>Bad Weather Alert</title>
          </head>
          <body>
              <h1>Bad Weather Alert</h1>
              <p>The following bad weather conditions are expected:</p>
              <ul>
                  weather_data
              </ul>
          </body>
          </html>
          """

        weather_items = []
        dt = None
        for item in bad_weather:
            if dt is None:
                dt = item[0].date()
                weather_items.append(f"<h4>{dt}</h4>")

            if dt < item[0].date():
                dt = item[0].date()
                weather_items.append(f"<h4>{dt}</h4>")

            weather_items.append(f"<li>Hour: {item[0].strftime("%H:%M")} | {item[1]} => "
                                 f"<strong><font color=\"Red\">{item[2]}</font></strong> - {item[3]}</li>")

        weather_data_str = "\n".join(weather_items)

        html_content = html_content.replace('weather_data', weather_data_str)
        # print(html_content)

        msg = MIMEMultipart()
        # msg = MIMEText(html_content)
        msg['Subject'] = "DEAC110-0923 - Bad Weather (Alert)"
        msg['From'] = "Bad Weather Alert <***@gmail.com>"
        msg['To'] = dest
        msg.attach(MIMEText(html_content, 'html'))

        s.sendmail("sender_email_id", dest, msg.as_string())
        s.quit()
        logger.info(f'.....email alert sent to {dest}')
