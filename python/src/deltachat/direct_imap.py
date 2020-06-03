import imaplib
import pathlib
from . import Account

INBOX = "Inbox"
SENT = "Sent"
MVBOX = "DeltaChat"
DC_CONSTANT_MSG_MOVESTATE_PENDING = 1
DC_CONSTANT_MSG_MOVESTATE_STAY = 2
DC_CONSTANT_MSG_MOVESTATE_MOVING = 3


def db_folder_attr(name):
    def fget(s):
        return s.db_folder.get(name, 1)

    def fset(s, val):
        s.db_folder[name] = val
    return property(fget, fset, None, None)


class ImapConn():
    def __init__(self, foldername, conn_info):
        self.foldername = foldername
        host, user, pw = conn_info

        self.connection = imaplib.IMAP4_SSL(host)
        self.connection.login(user, pw)
        messages = self.reselect_folder()
        try:
            self.original_msg_count = int(messages[0])
            print("dbg", str(messages))          
        except IndexError:
            self.original_msg_count = 0

    def mark_all_read(self):
        self.reselect_folder()
#        result, data = self.connection.uid('search', None, "(UNSEEN)")
        result, data = self.connection.search(None, 'UnSeen')
        try:
            mails_uid = data[0].split()
            print("New mails")

#            self.connection.store(data[0].replace(' ',','),'+FLAGS','\Seen')
            for e_id in mails_uid:
                self.connection.store(e_id, '+FLAGS', '\\Seen')
                print("marked:", e_id)

            return True
        except IndexError:
            print("No unread")
            return False

    def get_unread_cnt(self):
        self.reselect_folder()
#        result, data = self.connection.uid('search', None, "(UNSEEN)")
        result, data = self.connection.search(None, 'UnSeen')
        try:
            mails_uid = data[0].split()

            return len(mails_uid)
        except IndexError:
            return 0

    def get_new_email_cnt(self):
        messages = self.reselect_folder()
        try:
            return int(messages[0]) - self.original_msg_count
        except IndexError:
            return 0

    def reselect_folder(self):
        status, messages = self.connection.select(self.foldername)
        if status != "OK":
            print("Incorrect mail box " + status + str(messages))
            raise ConnectionError
#        print("(Re-)Selected mailbox: " + status + " " + str(messages))
        return messages

    def __del__(self):
        try:
            self.connection.close()
        except:
            pass
        try:
            self.connection.logout()
        except:
            pass
        

def make_direct_imap(account, folder):
    conn_info = (account.get_config("configured_mail_server"),
                 account.get_config("addr"), account.get_config("mail_pw"))
    imap = ImapConn(folder, conn_info=conn_info)
    return imap

def print_imap_structure(database):
    print_imap_structure_ac(Account(database))

def print_imap_structure_ac(ac):
    print("================= ACCOUNT", ac.get_config("addr"), "=================")
    print("----------------- CONFIG: -----------------")
    print(ac.get_info())

    for folder in [INBOX, MVBOX, SENT]:
        try:
            print("-----------------", folder, "-----------------")
            imap = make_direct_imap(ac, folder)
            c = imap.connection
            typ, data = c.search(None, 'ALL')
            c._get_tagged_response
            for num in data[0].split():
                typ, data = c.fetch(num, '(RFC822)')
                body = data[0][1]

                typ, data = c.fetch(num, '(UID FLAGS)')
                info = data[0]

                path = pathlib.Path("./IMAP-MESSAGES-" + ac.get_config("addr") + "~/" + folder)
                path.mkdir(parents=True, exist_ok=True)
                file = path.joinpath(str(info).replace("b'", "").replace("'", "").replace("\\", ""))
                file.write_bytes(body)
                print("Message", info, "saved as", file)
        except ConnectionError:
            print("Seems like there is no", folder, "folder")
