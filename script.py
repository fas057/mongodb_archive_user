import json
import os
from datetime import datetime
from pymongo import MongoClient

def archive_inactive_users():
   
    # Подключение к MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["my_database"]
    
    # Получаем ссылки на коллекции
    user_events = db["user_events"]       # Основная коллекция с пользователями
    archived_users = db["archived_users"] # Коллекция для архива

    # Текущая дата и время
    current_date = datetime.now()
    
    # Рассчитываем временные границы в секундах (Unix timestamp)
    current_timestamp = int(current_date.timestamp())
    # 30 дней в секундах (30 дней × 24 часа × 60 минут × 60 секунд)
    reg_threshold_sec = current_timestamp - (30 * 24 * 60 * 60)  
    # 14 дней в секундах
    activity_threshold_sec = current_timestamp - (14 * 24 * 60 * 60)  

    # Конвертируем секунды обратно в объекты datetime для запроса
    reg_threshold_date = datetime.fromtimestamp(reg_threshold_sec)
    activity_threshold_date = datetime.fromtimestamp(activity_threshold_sec)

    # Ищем пользователей для архивации:
    # 1. Дата регистрации раньше чем 30 дней назад
    # 2. Последняя активность раньше чем 14 дней назад
    inactive_users = user_events.find({
        "user_info.registration_date": {"$lt": reg_threshold_date},
        "event_time": {"$lt": activity_threshold_date}
    })

    # Преобразуем результат в список
    users_to_archive = list(inactive_users)

    # Если есть пользователи для архивации
    if users_to_archive:
        # Вставляем всех в архивную коллекцию
        archived_users.insert_many(users_to_archive)
        
        # Получаем ID всех заархивированных пользователей
        user_ids = [user["user_id"] for user in users_to_archive]
        
        # Удаляем их из основной коллекции
        user_events.delete_many({"user_id": {"$in": user_ids}})

    # Формируем отчет в виде словаря
    report = {
        "date": current_date.strftime("%Y-%m-%d"),  # Дата архивации
        "archived_users_count": len(users_to_archive),  # Количество пользователей
        "archived_user_ids": [user["user_id"] for user in users_to_archive]  # Список ID
    }

    # Создаем папку reports, если ее нет
    if not os.path.exists("reports"):
        os.makedirs("reports")

    # Формируем имя файла отчета (текущая дата)
    report_filename = f"reports/{current_date.strftime('%Y-%m-%d')}.json"
    
    # Записываем JSON файл
    with open(report_filename, "w") as f:
        json.dump(report, f, indent=4)  # indent для красивого форматирования

    # Вывод результата работы
    print(f"✅ Архивация завершена. Заархивировано пользователей: {len(users_to_archive)}")
    print(f"Отчет сохранен в файл: {report_filename}")

# Точка входа - выполняем функцию при запуске скрипта
if __name__ == "__main__":
    archive_inactive_users()
