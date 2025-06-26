import json
import os
from datetime import datetime
from pymongo import MongoClient

def archive_inactive_users():
    
    # Подключение к MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["my_database"]
    user_events = db["user_events"]
    archived_users = db["archived_users"]

    # Текущая дата
    current_date = datetime.now()
    current_sec = int(current_date.timestamp())
    
    # Пороговые значения (в секундах)
    reg_threshold_sec = current_sec - (30 * 24 * 60 * 60)  # 30 дней назад
    activity_threshold_sec = current_sec - (14 * 24 * 60 * 60)  # 14 дней назад

    # 1. Находим всех пользователей, зарегистрированных более 30 дней назад
    old_users = user_events.distinct("user_id", {
        "user_info.registration_date": {"$lt": datetime.fromtimestamp(reg_threshold_sec)}
    })

    # 2. Для каждого находим последнее событие
    users_to_archive = []
    for user_id in old_users:
        # Получаем последнее событие пользователя
        last_event = user_events.find_one(
            {"user_id": user_id},
            sort=[("event_time", -1)]  # Сортировка по убыванию даты
        )
        
        # Проверяем, что последнее событие было более 14 дней назад
        if last_event and last_event["event_time"].timestamp() < activity_threshold_sec:
            users_to_archive.append(last_event)

    # 3. Архивируем пользователей
    if users_to_archive:
        # Получаем полные данные всех событий для архивных пользователей
        user_ids = [user["user_id"] for user in users_to_archive]
        all_events_to_archive = list(user_events.find({"user_id": {"$in": user_ids}}))
        
        # Переносим в архив и удаляем из основной коллекции
        archived_users.insert_many(all_events_to_archive)
        user_events.delete_many({"user_id": {"$in": user_ids}})

    # 4. Формируем отчет
    report = {
        "date": current_date.strftime("%Y-%m-%d"),
        "archived_users_count": len(set(user_ids)) if users_to_archive else 0,
        "archived_user_ids": list(set(user_ids)) if users_to_archive else []
    }

    # Создаем папку для отчетов при необходимости
    os.makedirs("reports", exist_ok=True)
    
    # Сохраняем отчет
    report_filename = f"reports/{current_date.strftime('%Y-%m-%d')}.json"
    with open(report_filename, "w") as f:
        json.dump(report, f, indent=4)

    print(f"✅ Успешно заархивировано пользователей: {len(set(user_ids)) if users_to_archive else 0}")
    print(f"Отчет сохранен в: {report_filename}")

if __name__ == "__main__":
    archive_inactive_users()
