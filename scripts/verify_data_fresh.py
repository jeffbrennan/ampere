from ampere.common import get_frontend_db_con, get_secret
import smtplib


def send_email(body: str) -> None:
    subject = "[ALERT] Ampere Data Stale"

    sender = get_secret("AMPERE_BACKEND_EMAIL_FROM")
    password = get_secret("AMPERE_BACKEND_EMAIL_PW")
    recipient = get_secret("AMPERE_BACKEND_EMAIL_LIST")

    headers = [
        f"To: {recipient}",
        f"From: {sender}",
        f"Subject: {subject}",
        "MIME-Version: 1.0",
        "Content-Type: text/html",
    ]

    headers_str = "\r\n".join(headers)
    content = headers_str + "\r\n\r\n" + body
    print(content)

    mail = smtplib.SMTP("smtp.gmail.com", 587)
    mail.ehlo()
    mail.starttls()

    mail.login(sender, password)
    mail.sendmail(from_addr=sender, to_addrs=recipient, msg=content)

    mail.quit()


def check_freshness() -> None:
    con = get_frontend_db_con()
    results = (
        con.sql("""
            select *
            from int_status_summary
            where stale = true
        """)
        .to_df()
        .to_dict(orient="records")
    )

    if len(results) == 0:
        print("data is fresh")
        return

    formatted_content = "<br>".join(
        [
            f"STALE: {result['page']} [{result['hours_stale']} hours stale]"
            for result in results
        ]
    )

    send_email(formatted_content)


if __name__ == "__main__":
    check_freshness()
