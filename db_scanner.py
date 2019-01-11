#!/usr/bin/python3
import time
import sys
import mysql.connector as sql
import telepot
from db_cred import *
from gr_cred import *
from tokens import *
from telepot.namedtuple import ReplyKeyboardMarkup, KeyboardButton, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telepot.loop import MessageLoop

msc = []
proposed_exec = []
empty = False
checked = False

class proz(telepot.Bot):
   def __init__(self, *args, **kwargs):
      super(proz, self).__init__(*args, **kwargs)

   def queryExecutor(self, query):
      conn = sql.connect(user=db_user, password=db_pass, database=db_base)
      cur = conn.cursor(buffered=True)
      cur.execute(query)
      conn.commit()
      conn.close()
   
   def dataCollector(self, query):
      conn = sql.connect(user=db_user, password=db_pass, database=db_base)
      cur = conn.cursor(buffered=True)
      cur.execute(query)
      result = cur.fetchall()
      return result


   def initialScan(self):
      global msc
      global empty
      global checked
      while empty == False:
         task_query = ("select id, name, description from Tasks where status= 'Open' and id in (select task from Pendings where status = 'Open');")
         exec_query = ("SELECT id, name, chat_id FROM Executors where blacklisted=0 and id not in (SELECT executor FROM Pendings WHERE status = 'Pending') order by rating desc")

         tasks = bot.dataCollector(task_query)
         executors = bot.dataCollector(exec_query)
         for e in executors:
            print(e)

         if len(tasks) != 0:
            if len(executors) != 0:
               arr = [len(tasks), len(executors)]
               for i in range (min(arr)):
                  print("MINIMAL ARR: " + str(min(arr)))
                  print("INTEGER: " + str(i))
                  print("EXECUTORS: " + str(len(executors)))
                  print("TASKS: " + str(len(tasks)))
                  print("TASK_ID: " + str(tasks[i][0]))
                  print("TARGET POINT")
                  proposal = [executors[i][1], tasks[i][1], tasks[i][2]]
                  print("PROPOSED_EXEC_NAME: " + str(executors[i][1]))
                  print("TASK_ID: " + str(tasks[i][0]))
                  message = "Hello, " + executors[i][1] + "! Do you want to work with " + tasks[i][2] + "?"
                  keyboard = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text='Yes', callback_data='Yes' + str(executors[i][2])), InlineKeyboardButton(text='No', callback_data='No' + str(executors[i][2]))]])
                  msgc = bot.sendMessage(groupid, message, reply_markup=keyboard)
                  task_assign_query = ("update Pendings set executor = " + str(executors[i][0]) + ", answer = 'Empty', status = 'Pending' where task = " + str(tasks[i][0]) + ";")
                  bot.queryExecutor(task_assign_query)
                  time.sleep(1)
                  m = telepot.message_identifier(msgc)
                  msc.append([m[1], executors[i][0], executors[i][1], executors[i][2], tasks[i][0]])
#message_id, executor_id, #executor_name, executor_chat_id, task_id
                  print(msc)
                  checked = False
               else:
                     print("OUT OF CYCLE.")
            else:
               if checked == False:
                  bot.sendMessage(adminid, "Sorry, no free executors!")
                  time.sleep(3600)
         else:
            if checked == False:
               bot.sendMessage(adminid, "Sorry, no Open tasks!")
               time.sleep(3600)
         time.sleep(1)
   
   def on_callback_query(self, msg):
      global msc
      query_id, from_id, query_data = telepot.glance(msg, flavor='callback_query')
      print("LENGTH: " + str(len(msc)))
      for i in msc:
         print(i)
      print('Callback Query:', query_id, from_id, query_data)
      for i in msc:
         if str(i[3]) == str(from_id):
            print(i[3])
            if query_data == str("No" + str(from_id)) or query_data == str("Yes" + str(from_id)):
               print("TRYING TO DELETE MESSAGE_ID: " + str(i[0]))
               bot.deleteMessage((groupid, str(i[0])))
               msc.remove(i)
               if query_data == str('Yes' + str(from_id)):
                  bot.sendMessage(groupid, i[2] + ", you have accepted the proposal.")
                  acc_query = ("update Tasks set executor=" + str(i[1]) + ", status='InProgress' where id = " + str(i[4]) + ";")
                  bot.queryExecutor(acc_query)
                  pending_query = ("update Pendings set answer = 'Accepted', status = 'InProgress' where task = " + str(i[4]) + ";")
                  bot.queryExecutor(pending_query)
                  reply_query = ("insert into Replies (task, executor, answer) values (" + str(i[4]) + ", " + str(i[1]) + ", 'Accepted');")
                  bot.queryExecutor(reply_query)
                  translog_query = ("insert into TransactionLog (task, executor, answer, status) values (" + str(i[4]) + ", " + str(i[1])  + ", 'Accepted', 'InProgress');")
                  bot.queryExecutor(translog_query)
               elif query_data == str('No' + str(from_id)):
                  bot.sendMessage(groupid, i[2] + ", you have declined the proposal.")
                  pending_query = ("update Pendings set answer = 'Declined', status = 'Open' where task = " + str(i[4]) + ";")
                  print(pending_query)
                  bot.queryExecutor(pending_query)
                  reply_query = ("insert into Replies (task, executor, answer) values (" + str(i[4]) + ", " + str(i[1]) + ", 'Declined');")
                  bot.queryExecutor(reply_query)
                  translog_query = ("insert into TransactionLog (task, executor, answer, status) values (" + str(i[4]) + ", " + str(i[1])  + ", 'Declined', 'Open');")
                  bot.queryExecutor(translog_query)
            else:
               print("Error. " + query_data)


   def taskGenerator(self):
      gentask_query = ("insert into Tasks (name, description, manager, executor, client, rating, price, status) values ('RandomName', 'Random Description', 1, 0, 1, 0.1, 100, 'Open');")
      bot.queryExecutor(gentask_query)
      task_id_max_query = ("select max(id) from Tasks;")
      task_id = bot.dataCollector(task_id_max_query)
      print(task_id[0][0])
      pending_query = ("insert into Pendings (task, executor, answer, status) values (" + str(task_id[0][0]) + ", '0', 'Empty', 'Open');")
      bot.queryExecutor(pending_query)
      translog_query = ("insert into TransactionLog (task, executor, answer, status) values (" + str(task_id[0][0]) + ", 0, 'Empty', 'Open');")
      bot.queryExecutor(translog_query)


   def onChatMessage(self, msg):
      content_type, chat_type, chat_id = telepot.glance(msg)
      if chat_type == 'private':
         if msg['text'] == '/check' and chat_id == adminid:
            bot.sendMessage(adminid, "Silent Check is being written.")
         elif msg['text'] == '/gentask' and chat_id == adminid:
            bot.taskGenerator()
         elif msg['text'] == '/truncate' and chat_id == adminid:
            truncate_query1 = ("truncate table Tasks")
            truncate_query2 = ("truncate table Pendings")
            truncate_query3 = ("truncate table Replies")
            bot.queryExecutor(truncate_query1)
            bot.queryExecutor(truncate_query2)
            bot.queryExecutor(truncate_query3)

      elif str(msg['chat']['id']) == groupid:
         bot.sendMessage(groupid, "Sorry, I don't understand you yet.")
      else:
            bot.sendMessage(chat_id, "Sorry, I do not have permission to answer.")


TOKEN = telegrambot
bot = proz(TOKEN)
MessageLoop(bot, {'chat': bot.onChatMessage, 'callback_query': bot.on_callback_query}).run_as_thread()
bot.initialScan()
print('Listening ...')

while 1:
   time.sleep(10)
