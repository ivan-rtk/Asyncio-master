import sqlite3
from sqlite3 import Error
import datetime
import aiosmtplib
from email.message import EmailMessage
import asyncio
from more_itertools import chunked


def create_connection(db_file_):
    conn = None
    print(conn)
    try:
        conn = sqlite3.connect(db_file_)
    except Error as e:
        print(e)
    return conn


async def send_to_contacts(contact: tuple):
    message = EmailMessage()
    message["From"] = "root@localhost"
    message["To"] = contact[3]
    message["Subject"] = "Здравствуйте"
    message.set_content(f"Уважаемый {contact[1]} {contact[2]}\nСпасибо, что пользуетесь нашим сервисом объявлений.")
    await aiosmtplib.send(message, hostname="smtp.mail.ru", port=465, use_tls=True, username="email", password="password")
    print("succes", str(datetime.datetime.now()))


async def main():
    db_file = 'contacts.db'
    conn = create_connection(db_file)
    cur = conn.cursor()
    cur.execute("SELECT * FROM contacts")
    contacts = cur.fetchall()
    for chunk in chunked(contacts, 10):
        print(list(chunk))
        tasks = [asyncio.create_task(send_to_contacts(contact)) for contact in chunk]
        results = await asyncio.gather(*tasks)
        print(results)
    cur.close()

event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(main())