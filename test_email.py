from app.services.email_service import send_email

if __name__ == "__main__":
    send_email(
        to_email=["1irfanakgul@gmail.com",'Pidatasmanba@gmail.com','yasinyilmaz19@gmail.com'],
        subject="TEST MAIL VIA PYTHON",
        body="Hi Guys, this e-mail has been sent by python code in order to do some test for future :) please reply if you read this e-mail :)"
    )
    print("Mail gönderildi.")