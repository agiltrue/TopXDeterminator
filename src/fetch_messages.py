import requests
from datetime import datetime
import time
import csv
import re
import os


# === Конфигурация ===
URL = "https://api-time.tinkoff.ru/api/v4"
CHANNEL_ID = os.getenv("TIME_CHANNEL_ID")  # канал задаём через переменную окружения
TOKEN = os.getenv("TIME_TOKEN")            # токен тоже через env-переменную
API_URL = f"{URL}/channels/{CHANNEL_ID}/posts"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}


# === Вспомогательные функции ===
def fetch_posts(before=None):
    """Запрашиваем пачку сообщений из канала"""
    params = {
        "page": 0,
        "per_page": 100,
        "skipFetchThreads": "true",
        "collapsedThreads": "true"
    }
    if before:
        params["before"] = before

    response = requests.get(API_URL, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


def get_all_posts_until(target_date_str):
    """Выгружаем все сообщения до указанной даты"""
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    all_posts = {}
    last_post_id = None

    while True:
        data = fetch_posts(before=last_post_id)
        posts = data.get("posts", {})
        if not posts:
            break

        sorted_posts = sorted(posts.items(), key=lambda x: x[1]["create_at"], reverse=True)
        for post_id, post in sorted_posts:
            post_date = datetime.fromtimestamp(post["create_at"] / 1000)
            if post_date < target_date:
                return all_posts
            all_posts[post_id] = post

        last_post_id = sorted_posts[-1][0]
        time.sleep(0.3)  # чтобы не перегружать API

    return all_posts


def parse_message(msg):
    """Парсим одно сообщение"""
    text = msg["message"]

    # Воркфлоу = первый жирный заголовок
    workflow_match = re.match(r"\*\*(.*?)\*\*", text)
    workflow = workflow_match.group(1).strip() if workflow_match else ""

    # Автор
    author_match = re.search(r"от пользователя (.*)", text)
    author = author_match.group(1).strip() if author_match else ""

    # Сервис
    service_match = re.search(r"\*\*Сервис\*\*:\s*\n*(.*?)\s*\n\*\*", text, re.DOTALL)
    if not service_match:
        service_match = re.search(r"\*\*По какому сервису вопрос.*\*\*:\s*\n*(.*?)\n", text, re.DOTALL)
    service = service_match.group(1).strip() if service_match else ""

    # Срочность
    urgency_match = re.search(r"\*\*Срочность\*\*:\s*\n*(.*?)\n", text, re.DOTALL)
    urgency = urgency_match.group(1).strip() if urgency_match else ""

    # Тип проблемы
    problem_type_match = re.search(r"\*\*Тип проблемы\*\*:\s*\n*(.*?)\n", text, re.DOTALL)
    problem_type = problem_type_match.group(1).strip() if problem_type_match else ""

    # Вопрос
    question_match = re.search(r"\*\*Вопрос.*?\*\*:\s*\n*(.*?)\n\*\*", text, re.DOTALL)
    question = question_match.group(1).strip() if question_match else text

    return {
        "id": msg["id"],
        "date_created": datetime.fromtimestamp(msg["create_at"] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
        "author": author,
        "workflow": workflow,
        "service": service,
        "urgency": urgency,
        "problem_type": problem_type,
        "question_text": question,
        "raw_message": text
    }


def filter_workflow_messages(posts):
    """Фильтруем только обращения по дебетовым картам/продуктам"""
    extracted = []
    for post_id, msg in posts.items():
        text = msg["message"]
        if text.startswith("**Дебетовые") or text.startswith("**Обращение в top-deposit"):
            if not (text.startswith("**Дебетовые карты - WEB") or text.startswith("**Дебетовые карты - Мобильный банк")):
                extracted.append(parse_message(msg))
    return extracted


def save_to_csv(data, filename="data/raw/debit_cards_dataset.csv"):
    """Сохраняем в CSV"""
    if not data:
        return
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    fieldnames = data[0].keys()
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


# === Запуск ===
if __name__ == "__main__":
    target_date = "2022-01-01"  # от какой даты тянуть
    all_posts = get_all_posts_until(target_date)
    dataset = filter_workflow_messages(all_posts)
    save_to_csv(dataset)
    print(f"Сохранено {len(dataset)} сообщений в data/raw/debit_cards_dataset.csv")
