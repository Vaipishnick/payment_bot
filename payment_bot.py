import asyncio
import math
import os
import sqlite3
import logging
from datetime import datetime, timedelta

from telethon import TelegramClient, events
from telethon.errors import ChatAdminRequiredError
from telethon.tl.types import ChannelParticipantsAdmins
from dotenv import load_dotenv

# ------------------------- Настройка логирования -------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ------------------------- Конфигурация -------------------------
load_dotenv()
api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')
bot_token = os.getenv('BOT_TOKEN')
group_id = int(os.getenv('GROUP_ID'))
SESSION_NAME = 'bot_session'

# ------------------------- Работа с базой данных -------------------------
def init_db():
    """Создаёт таблицы с правильной структурой (total_amount вместо amount)."""
    conn = sqlite3.connect('payment.db')
    conn.execute('PRAGMA foreign_keys = ON')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            user_name TEXT
        )
    ''')

    # Таблица payments: храним общую сумму (total_amount), текущую долю (payment),
    # дату последнего события (last_payment) и количество месяцев (months) - как вычисляемое поле для удобства
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            user_id INTEGER PRIMARY KEY,
            last_payment TEXT,
            payment INTEGER,
            total_amount INTEGER DEFAULT 0,
            months INTEGER DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )
    ''')

    # Проверяем наличие столбца total_amount (для совместимости со старыми БД)
    cursor.execute("PRAGMA table_info(payments)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'total_amount' not in columns:
        cursor.execute("ALTER TABLE payments ADD COLUMN total_amount INTEGER DEFAULT 0")
        # Если было поле amount, перенесём данные (для существующих установок)
        if 'amount' in columns:
            cursor.execute("UPDATE payments SET total_amount = amount WHERE amount IS NOT NULL")
            # Можно удалить старое поле, но для простоты оставим

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value INTEGER
        )
    ''')
    cursor.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ('rent_amount', 210))

    conn.commit()
    conn.close()

def get_rent_amount():
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('SELECT value FROM settings WHERE key = ?', ('rent_amount',))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else 210

def set_rent_amount(value):
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE settings SET value = ? WHERE key = ?', (value, 'rent_amount'))
    conn.commit()
    conn.close()

def add_or_update_user(user_id, user_name):
    """Добавляет пользователя или обновляет имя. Возвращает True, если пользователь новый."""
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM users WHERE user_id = ?', (user_id,))
    exists = cursor.fetchone() is not None

    if exists:
        cursor.execute('UPDATE users SET user_name = ? WHERE user_id = ?', (user_name, user_id))
        conn.commit()
        conn.close()
        return False
    else:
        cursor.execute('INSERT INTO users (user_id, user_name) VALUES (?, ?)', (user_id, user_name))
        conn.commit()
        conn.close()
        return True

def change_user_amount(user_name, new_amount):
    """Изменяет сумму внесенную пользователем"""
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('SELECT user_id FROM users WHERE user_name = ?', (user_name,))
    user_id = cursor.fetchone()
    if user_id is None:
        return "Пользователь " + user_name + " не существует"
        conn.commit()
        conn.close()
    else:
        update_statement = "UPDATE payments SET total_amount = ? WHERE user_id == ?"
        cursor.execute(update_statement, (new_amount, user_id[0],))
        conn.commit()
        conn.close()
        return f"Пользователь {user_name} найден, остаток на счете изменен на {new_amount}"

def ensure_payment_record(user_id):
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO payments (user_id, total_amount, months) VALUES (?, ?, ?)',
                   (user_id, 0, 0))
    conn.commit()
    conn.close()

def get_user_data(user_id):
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT u.user_id, u.user_name, p.last_payment, p.payment, p.total_amount, p.months
        FROM users u
        LEFT JOIN payments p ON u.user_id = p.user_id
        WHERE u.user_id = ?
    ''', (user_id,))
    row = cursor.fetchone()
    conn.close()
    return row

def add_user_payment(user_id, amount, payment, last_payment_date):
    """Добавляет сумму к total_amount, пересчитывает months."""
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    # Получаем текущий total_amount
    cursor.execute('SELECT total_amount FROM payments WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    current_total = row[0] if row and row[0] is not None else 0

    new_total = current_total + amount
    new_months = new_total // payment

    cursor.execute('''
        UPDATE payments
        SET total_amount = ?, last_payment = ?, payment = ?, months = ?
        WHERE user_id = ?
    ''', (new_total, last_payment_date, payment, new_months, user_id))
    conn.commit()
    conn.close()

def update_user_after_spending(user_id, new_total, new_months, new_last_payment):
    """Обновляет total_amount, months и last_payment после списания месяцев."""
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE payments
        SET total_amount = ?, months = ?, last_payment = ?
        WHERE user_id = ?
    ''', (new_total, new_months, new_last_payment, user_id))
    conn.commit()
    conn.close()

def update_all_payments(new_payment):
    """Обновляет поле payment для всех пользователей."""
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE payments SET payment = ?', (new_payment,))
    conn.commit()
    conn.close()

def recalc_all_months():
    """Пересчитывает months для всех пользователей: months = total_amount // payment."""
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, total_amount, payment FROM payments WHERE payment IS NOT NULL AND payment > 0')
    rows = cursor.fetchall()
    for user_id, total_amount, payment in rows:
        if total_amount is None:
            total_amount = 0
        new_months = total_amount // payment
        cursor.execute('UPDATE payments SET months = ? WHERE user_id = ?', (new_months, user_id))
    conn.commit()
    conn.close()

def get_all_payments():
    """Возвращает список (user_id, last_payment, total_amount, payment, months)."""
    conn = sqlite3.connect('payment.db')
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, last_payment, total_amount, payment, months FROM payments')
    rows = cursor.fetchall()
    conn.close()
    return rows

# ------------------------- Вспомогательные функции -------------------------
async def count_group_members(client):
    try:
        group = await client.get_entity(group_id)
        count = 0
        async for user in client.iter_participants(group):
            if not user.bot:
                count += 1
        return count
    except Exception as e:
        logger.error(f"Ошибка подсчёта участников: {e}")
        return 1

async def get_current_payment(client):
    rent = get_rent_amount()
    users_count = await count_group_members(client)
    return math.ceil(rent / users_count) if users_count > 0 else rent

async def refresh_all_payments(client):
    new_payment = await get_current_payment(client)
    update_all_payments(new_payment)
    logger.info(f"Payment всех пользователей обновлён: {new_payment}")

async def is_admin(event, client):
    """Проверяет, является ли отправитель администратором группы."""
    try:
        sender = await event.get_sender()
        if sender.bot:
            return False
        group = await client.get_entity(group_id)
        admins = await client.get_participants(group, filter=ChannelParticipantsAdmins)
        admin_ids = [admin.id for admin in admins]
        return sender.id in admin_ids
    except Exception as e:
        logger.error(f"Ошибка проверки прав администратора: {e}")
        return False

async def send_private_reply(event, client, text, parse_mode='markdown', fallback_to_group=True, group_notification=True):
    """
    Отправляет ответ пользователю в личные сообщения.
    Параметры:
        event - объект события (содержит информацию об отправителе и чате)
        client - экземпляр TelegramClient
        text - текст сообщения
        parse_mode - режим разметки (по умолчанию 'markdown')
        fallback_to_group - если True и не удалось отправить в личку,
                            а команда из группы, ответ будет продублирован в группу
        group_notification - если True и команда из группы, после успешной отправки
                             в личку отправить краткое уведомление в группу
    Возвращает:
        True, если сообщение успешно отправлено в личку,
        False в противном случае.
    """
    user_id = event.sender_id
    is_group = event.is_group

    try:
        await client.send_message(user_id, text, parse_mode=parse_mode)
        if is_group and group_notification:
            await event.reply("📩 Я отправил вам личное сообщение с данными.")
        return True
    except Exception as e:
        logger.error(f"Не удалось отправить личное сообщение пользователю {user_id}: {e}")

        if is_group and fallback_to_group:
            await event.reply(text, parse_mode=parse_mode)
            return False
        elif not is_group:
            await event.reply("❌ Не удалось отправить сообщение. Возможно, я ещё не могу писать вам. Напишите /start в личку, чтобы начать диалог.")
            return False
        else:
            await event.reply("❌ Не удалось отправить вам личное сообщение. Проверьте, не заблокирован ли бот, и напишите ему /start в личку.")
            return False

# ------------------------- Сбор участников при старте -------------------------
async def collect_group_members(client):
    try:
        group = await client.get_entity(group_id)
        logger.info(f"Группа: {group.title} (ID: {group.id})")
        logger.info("Начинаю сбор участников...")
        async for user in client.iter_participants(group):
            if not user.bot:
                add_or_update_user(user.id, user.username)
                ensure_payment_record(user.id)
        logger.info("Сбор участников завершён.")
    except ChatAdminRequiredError:
        logger.error("Ошибка: бот не является администратором группы!")
    except Exception as e:
        logger.error(f"Ошибка при сборе участников: {type(e).__name__}: {e}")

# ------------------------- Фоновая задача (раз в неделю) -------------------------
async def weekly_check(client):
    while True:
        await asyncio.sleep(7 * 24 * 60 * 60)  # 7 дней
        logger.info("Запуск еженедельной проверки списания месяцев...")
        today = datetime.now()
        payments = get_all_payments()
        for user_id, last_payment_str, total_amount, payment, months in payments:
            if last_payment_str is None or total_amount is None or total_amount <= 0 or payment is None or payment <= 0:
                continue

            last_payment = datetime.strptime(last_payment_str, '%Y-%m-%d')
            days_passed = (today - last_payment).days
            if days_passed >= 30:
                months_passed = days_passed // 30
                # Уменьшаем total_amount на payment * months_passed
                new_total = total_amount - payment * months_passed
                if new_total < 0:
                    new_total = 0
                new_last_payment = last_payment + timedelta(days=months_passed * 30)
                new_last_payment_str = new_last_payment.strftime('%Y-%m-%d')
                new_months = new_total // payment

                update_user_after_spending(user_id, new_total, new_months, new_last_payment_str)

                if new_months < 1:
                    try:
                        user_data = get_user_data(user_id)
                        if user_data:
                            user_name = user_data[1] or f"id{user_id}"
                            mention = f"[{user_name}](tg://user?id={user_id})"
                            await client.send_message(
                                group_id,
                                f"⚠️ {mention}, ваш баланс месяцев закончился. Пожалуйста, внесите оплату.",
                                parse_mode='markdown'
                            )
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")

        logger.info("Еженедельная проверка завершена.")

# ------------------------- Обработчики команд -------------------------
def register_handlers(client):
    @client.on(events.NewMessage(pattern=r'^/start$'))
    async def start_handler(event):
        await event.reply(
            "Привет! Я бот для учёта платежей в группе.\n"
            "Используй /help, чтобы увидеть список команд."
        )

    @client.on(events.NewMessage(pattern=r'^/help$'))
    async def help_handler(event):
        help_text = (
            "📋 **Доступные команды**\n"
            "/mydata – показать ваши данные\n"
            "/mymonths – показать количество оплаченных месяцев\n"
            "/payment – показать текущую стоимость одного месяца\n"
            "/pay <сумма> – внести платёж (сумма числом)\n"
            "/setrent <сумма> – установить новую сумму аренды сервера (только для админов)\n"
            "/info – текущие настройки и статистика\n"
            "/debtors – список должников (только для админов)\n"
            "/change_amount <имя> <сумма>- изменить остаток на счете (только для админов)\n"
            "/help – эта справка"
        )
        await event.reply(help_text)

    @client.on(events.NewMessage(pattern=r'^/mydata$'))
    async def mydata_handler(event):
        user_id = event.sender_id
        sender = await event.get_sender()
        user_name = sender.username or f"id{user_id}"

        is_new = add_or_update_user(user_id, user_name)
        ensure_payment_record(user_id)

        if is_new:
            await refresh_all_payments(client)

        data = get_user_data(user_id)
        if data:
            _, name, last_payment, payment, total_amount, months = data
            # Формируем корректное упоминание
            mention = f"[{name}](tg://user?id={user_id})"
            response = (
                f"👤 **Пользователь:** {mention}\n"
                f"📅 **Последний платёж/списание:** {last_payment or 'не указан'}\n"
                f"💰 **Ваша доля (payment):** {payment if payment is not None else 'не рассчитана'}\n"
                f"💵 **Общая внесённая сумма:** {total_amount if total_amount is not None else 0}\n"
                f"📆 **Оплачено месяцев (months):** {months}"
            )
        else:
            response = "Не удалось найти ваши данные."

        await send_private_reply(event, client, response, fallback_to_group=False, group_notification=True)

    @client.on(events.NewMessage(pattern=r'^/mymonths$'))
    async def mymonths_handler(event):
        user_id = event.sender_id
        sender = await event.get_sender()
        user_name = sender.username or f"id{user_id}"

        add_or_update_user(user_id, user_name)
        ensure_payment_record(user_id)

        data = get_user_data(user_id)
        if data and data[5] is not None:
            months = data[5]
            text = f"📆 У вас осталось оплаченных месяцев: **{months}**"
        else:
            text = "❌ Информация о месяцах отсутствует."

        await send_private_reply(event, client, text, fallback_to_group=False, group_notification=True)

    @client.on(events.NewMessage(pattern=r'^/(payment|monthly)$'))
    async def payment_handler(event):
        user_id = event.sender_id
        sender = await event.get_sender()
        user_name = sender.username or f"id{user_id}"
        add_or_update_user(user_id, user_name)
        ensure_payment_record(user_id)
        current_payment = await get_current_payment(client)
        await event.reply(f"💰 Текущая стоимость одного месяца (payment): **{current_payment}**")

    @client.on(events.NewMessage(pattern=r'^/pay\s+(\d+)$'))
    async def pay_handler(event):
        user_id = event.sender_id
        amount = int(event.pattern_match.group(1))
        if amount <= 0:
            await event.reply("❌ Сумма должна быть положительным числом.")
            return

        today = datetime.now().strftime('%Y-%m-%d')

        sender = await event.get_sender()
        user_name = sender.username or f"id{user_id}"

        is_new = add_or_update_user(user_id, user_name)
        ensure_payment_record(user_id)

        if is_new:
            await refresh_all_payments(client)

        # Получаем текущую долю
        current_payment = await get_current_payment(client)

        # Добавляем платёж
        add_user_payment(user_id, amount, current_payment, today)

        # Получаем обновлённые данные для ответа
        data = get_user_data(user_id)
        _, _, _, _, total_amount, months = data

        response = (
            f"✅ Платёж зарегистрирован!\n"
            f"📅 Дата: {today}\n"
            f"💵 Внесено: {amount}\n"
            f"💰 Ваша доля (актуальная): {current_payment}\n"
            f"💳 Общая сумма на счету: {total_amount}\n"
            f"📆 Оплачено месяцев: {months}"
        )

        await send_private_reply(event, client, response, fallback_to_group=False, group_notification=True)

    @client.on(events.NewMessage(pattern=r'^/setrent\s+(\d+)$'))
    async def setrent_handler(event):
        # Проверка прав администратора
        if not await is_admin(event, client):
            await event.reply("❌ Эта команда доступна только администраторам группы.")
            return

        new_rent = int(event.pattern_match.group(1))
        if new_rent <= 0:
            await event.reply("❌ Сумма аренды должна быть положительным числом.")
            return

        set_rent_amount(new_rent)

        await refresh_all_payments(client)
        recalc_all_months()

        await event.reply(f"✅ Сумма аренды изменена на {new_rent}. Доли и месяцы всех пользователей обновлены.")

    @client.on(events.NewMessage(pattern=r'^/change_amount\s+(\w+)\s+(\d+)$'))
    async def change_amount_handler(event):
        # Проверка прав администратора
        if not await is_admin(event, client):
            await event.reply("❌ Эта команда доступна только администраторам группы.")
            return
        user_name = event.pattern_match.group(1)
        new_amount = int(event.pattern_match.group(2))
        if new_amount <= 0:
            await event.reply("❌ Сумма аренды должна быть положительным числом.")
            return

        result = change_user_amount(user_name, new_amount)

        await refresh_all_payments(client)
        recalc_all_months()

        await event.reply(f"{result}")

    @client.on(events.NewMessage(pattern=r'^/info$'))
    async def info_handler(event):
        rent = get_rent_amount()
        users_count = await count_group_members(client)
        current_payment = math.ceil(rent / users_count) if users_count > 0 else rent
        await event.reply(
            f"📊 **Текущая статистика**\n"
            f"💰 Сумма аренды: {rent}\n"
            f"👥 Участников (людей): {users_count}\n"
            f"💸 Доля каждого (payment): {current_payment}"
        )

    @client.on(events.NewMessage(pattern=r'^/(pay|setrent)$'))
    async def missing_arg_handler(event):
        await event.reply("❌ Команда требует аргумент. Пример: /pay 500")

# ------------------------- Основная функция -------------------------
async def main():
    init_db()

    client = TelegramClient(SESSION_NAME, api_id, api_hash)
    await client.start(bot_token=bot_token)
    logger.info("Бот запущен и слушает команды...")

    await collect_group_members(client)
    await refresh_all_payments(client)

    register_handlers(client)

    asyncio.create_task(weekly_check(client))

    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
