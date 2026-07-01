"""Course content (video IDs and topic timecodes), for all BilimBook courses.

This data used to live in a public `data.js` bundled with the frontend, which
leaked the (unlisted) YouTube video IDs to anyone with the WebApp URL. It now
lives in the backend and is served only to authenticated, entitled users via
`GET /api/course?course_id=...` (see bot.py), after their Telegram `initData`
has been verified and their `user_course_access` row checked.

All timecodes are stored as plain integer seconds. `endSeconds` is derived
automatically from the start of the next topic so the player can scale its
progress bar to the current topic's segment (see the "Этап 6" logic in
bos-course/main.js). The last topic of a video has `endSeconds = None`,
meaning "play until the end of the video".

`COURSES` is the registry of course content, keyed by course_id (matching
the `courses` table in Supabase). To add a new course, add a new
`_build_<course>_course()` function and register it below.
"""

import re
from typing import Optional

R2_PUBLIC_URL = "https://pub-633ad4e98b3c43a1a84f5168e7d6b219.r2.dev"

_YOUTUBE_ID_RE = re.compile(r"(?:youtu\.be/|[?&]v=|/embed/)([^&?/]{11})")


def _video_id(url: str) -> str:
    match = _YOUTUBE_ID_RE.search(url)
    if not match:
        raise ValueError(f"Cannot extract YouTube video ID from URL: {url!r}")
    return match.group(1)


def _build_topics(raw_topics: list[dict]) -> list[dict]:
    topics = []
    for i, raw in enumerate(raw_topics):
        start = raw["start"]
        end: Optional[int] = raw_topics[i + 1]["start"] if i + 1 < len(raw_topics) else None
        topics.append({"title": raw["title"], "startSeconds": start, "endSeconds": end})
    return topics


def _validate_chronology(label: str, topics: list[dict]) -> None:
    for i in range(1, len(topics)):
        prev, cur = topics[i - 1], topics[i]
        if cur["startSeconds"] <= prev["startSeconds"]:
            raise ValueError(
                f"{label}: topic {i} ({cur['title']!r}, {cur['startSeconds']}s) "
                f"is not after topic {i - 1} ({prev['title']!r}, {prev['startSeconds']}s)"
            )


_RAW_DAYS = [
    {
        "id": 1,
        "title": "День 1",
        "videoUrl": "https://youtu.be/egwkEl7Ejcg",
        "topics": [
            {"title": "Приветствие, знакомство, регламент", "start": 0},
            {"title": "Александр Высоцкий — биография", "start": 976},
            {"title": "О бизнес-бустере — программа 6 дней", "start": 1392},
            {"title": "Партнерство, ошибки совладельцев", "start": 2434},
            {"title": "Функции владельца бизнеса", "start": 3021},
            {"title": "Проблемы ручного управления и почти системы", "start": 3279},
            {"title": "Примеры Nintendo, Kodak, Apple, адвайзер-борд", "start": 3745},
            {"title": "Оргструктура, задание, QnA", "start": 4550},
            {"title": "Три роли: владелец, директор, специалист", "start": 4821},
            {"title": "Первая и вторая стадии: Стартап и ручное управление", "start": 5296},
            {"title": "Третья стадия: Почти система", "start": 6208},
            {"title": "Четвертая и пятая стадии: Системная компания и масштабирование", "start": 7022},
            {"title": "Почему важно понимать стадию своего бизнеса", "start": 7445},
            {"title": "Итоги дня, задания и организационные вопросы", "start": 8542},
            {"title": "Практическое задание — знакомство", "start": 9985},
        ],
    },
    {
        "id": 2,
        "title": "День 2",
        "videoUrl": "https://youtu.be/B5bs97rra4E",
        "topics": [
            {"title": "Вступление и анонс дня", "start": 0},
            {"title": "Разбор кейсов задания 2.1", "start": 91},
            {"title": "Итоги и саморефлексия по делегированию", "start": 1382},
            {"title": "Оргструктура, планирование, найм и скорость роста", "start": 1814},
            {"title": "Недельное планирование — основные принципы", "start": 2050},
            {"title": "Дожим клиента и продукт сотрудника", "start": 2186},
            {"title": "Продукт сотрудника — примеры по должностям", "start": 3041},
            {"title": "Функциональная структура — введение", "start": 4143},
            {"title": "Организация, функции и роль руководителя", "start": 4581},
            {"title": "Упущенные функции — пример магазина", "start": 5370},
            {"title": "Орг. структура из 7 департаментов", "start": 5716},
            {"title": "После найма — введение в должность", "start": 6020},
            {"title": "Два вида линий взаимодействия и бюрократия", "start": 6241},
            {"title": "Функциональная структура и департаменты", "start": 7127},
            {"title": "Внедрение: красный паук и перегруз владельца", "start": 7746},
            {"title": "Найм под область — 4 фактора отбора", "start": 8214},
            {"title": "Четыре ошибки до найма", "start": 8714},
            {"title": "Цикл найма — от объявления до испытательного срока", "start": 9162},
            {"title": "Найм как функция: расчёт потребности в HR", "start": 9843},
            {"title": "Причина и следствие в планировании", "start": 10222},
            {"title": "Почему люди боятся планировать", "start": 10610},
            {"title": "Стратегический, тактический и оперативный уровни", "start": 10791},
            {"title": "Формат недельного плана и координации", "start": 11342},
            {"title": "Переход к практической части дня 2", "start": 11783},
            {"title": "Оргструктура из 21 отдела", "start": 12102},
            {"title": "Уровни планирования", "start": 12300},
            {"title": "Быстрый найм и введение в должность", "start": 12500},
            {"title": "Ответы на вопросы из чата", "start": 12700},
        ],
    },
    {
        "id": 3,
        "title": "День 3",
        "videoUrl": "https://youtu.be/_CoL2l96wo0",
        "topics": [
            {"title": "Открытие дня", "start": 0},
            {"title": "История с Леной — проблема без метрик", "start": 180},
            {"title": "Что такое метрика — пример с лектором", "start": 2921},
            {"title": "Метрики бизнес-процесса: пример с воронкой продаж", "start": 3262},
            {"title": "Рациональность сотрудников и роль метрик", "start": 4024},
            {"title": "Личные метрики и еженедельный цикл управления", "start": 4441},
            {"title": "Вопросы из чата: внедрение и автоматизация метрик", "start": 4785},
            {"title": "Личный пример: метрика качества", "start": 6668},
            {"title": "Сложная метрика: ПР и имидж компании", "start": 7385},
            {"title": "Метрики и выход из операционки", "start": 8287},
            {"title": "Координация — определение и примеры", "start": 8377},
            {"title": "Функциональная координация", "start": 8663},
            {"title": "Три принципа успешной координации", "start": 9029},
            {"title": "Задание 3.1 — График валового дохода", "start": 9948},
            {"title": "QnA — Мотивация и зарплата", "start": 10267},
            {"title": "Задание 3.2 — Список координаций", "start": 10337},
            {"title": "QnA — Диагностика и структура отделов", "start": 11240},
            {"title": "QnA — Метрики для конкретных должностей", "start": 11880},
            {"title": "QnA — Мотивация, найм и сезонность", "start": 12625},
            {"title": "QnA — Сезонность и координации", "start": 13074},
            {"title": "QnA — Диагностика и завершение", "start": 13525},
        ],
    },
    {
        "id": 4,
        "title": "День 4",
        "videoUrl": "https://youtu.be/jEjx6_iJKrU",
        "topics": [
            {"title": "Введение и подготовка к практическому занятию", "start": 0},
            {"title": "Разбор заданий по предыдущему дню", "start": 105},
            {"title": "Анализ успешных действий и ошибок — Александр", "start": 259},
            {"title": "Сезонность и ошибки в ценообразовании", "start": 551},
            {"title": "Неэффективные продажи — Олег", "start": 1007},
            {"title": "Реальные кейсы и осознания участников", "start": 1470},
            {"title": "Начало вебинара по маркетингу и продажам", "start": 1568},
            {"title": "Как систематизировать маркетинг", "start": 1651},
            {"title": "Основы маркетинга — управление производством", "start": 1982},
            {"title": "Главный маркетинговый вопрос и ошибки", "start": 2137},
            {"title": "Примеры провалов в маркетинговой стратегии", "start": 2349},
            {"title": "Заземление — контакт с рынком", "start": 2605},
            {"title": "Анализ конкурентов — быстрый способ", "start": 2805},
            {"title": "Описание целевой аудитории", "start": 3132},
            {"title": "Боли целевой аудитории — сбор информации", "start": 3270},
        ],
    },
    {
        "id": 5,
        "title": "День 5",
        "videoUrl": "https://youtu.be/w3HwgULY1lA",
        "topics": [
            {"title": "Открытие дня и знакомство с Алексеем", "start": 0},
            {"title": "Разбор кейсов — маркетинг и продажи", "start": 86},
            {"title": "Осознания участников и итоги маркетинга", "start": 705},
            {"title": "Удача и цели владельца", "start": 1897},
            {"title": "Система управления финансами — Вводная", "start": 2115},
            {"title": "Почему владелец застревает в операционке финансов", "start": 2727},
            {"title": "Разделение счетов — основа системы", "start": 3119},
            {"title": "Финансовая модель распределения средств", "start": 3239},
            {"title": "Еженедельное планирование и Рекомендательный совет", "start": 3468},
            {"title": "Полная прозрачность финансов для руководителей", "start": 3600},
            {"title": "Обязанности владельца — Финансовый инструмент", "start": 3848},
            {"title": "Практическое задание и бонусы", "start": 4080},
            {"title": "Ответы на вопросы", "start": 4320},
        ],
    },
    {
        "id": 6,
        "title": "День 6",
        "videoUrl": "https://youtu.be/oydArwTtshg",
        "topics": [
            {"title": "Открытие дня — recap", "start": 0},
            {"title": "Разбор заданий по финансам", "start": 125},
            {"title": "Осознания участников и переход к новой теме", "start": 1363},
            {"title": "Как внедрять изменения", "start": 1627},
            {"title": "Идеология, оргсхема и метрики", "start": 2139},
            {"title": "Проблемы делегирования и кейс Сардора", "start": 4155},
            {"title": "Найм — следующий инструмент масштабирования", "start": 4679},
            {"title": "Планирование и координация руководителей", "start": 5109},
            {"title": "Координация и управление финансами", "start": 5659},
            {"title": "Управление продажами и маркетинг", "start": 6078},
            {"title": "Ошибки внедрения", "start": 6527},
            {"title": "Платформа Бизнес Бустер", "start": 8522},
            {"title": "Структура программы — 5 уровней", "start": 10818},
            {"title": "Варианты участия и предложение", "start": 11019},
            {"title": "QnA — Старт без опыта, время и тарифы", "start": 11779},
            {"title": "Призыв к действию и отзыв выпускницы", "start": 13051},
            {"title": "Бизнес-инженер — отбор, уровни и бонусы", "start": 13427},
            {"title": "QnA с бизнес-инженером и прощание", "start": 14323},
        ],
    },
]

_RAW_ROADMAP = {
    "id": "roadmap",
    "title": "Дорожная карта: 12 шагов (live)",
    "videoUrl": "https://youtu.be/S_9NgQgwXRM",
    "topics": [
        {"title": "Вступление — проблемы владельца и анонс вебинара", "start": 0},
        {"title": "Об Александре Высоцком — миссия и опыт", "start": 329},
        {"title": "Личный путь — история создания системного бизнеса", "start": 609},
        {"title": "Обзор дорожной карты: 12 шагов к системному бизнесу", "start": 2139},
        {"title": "Шаг 1 — Функциональная структура", "start": 2356},
        {"title": "Шаг 2 — Система управления задачами", "start": 2768},
        {"title": "Шаг 3 — Пульс команды", "start": 3217},
        {"title": "Шаг 4 — Финансы под ключ", "start": 3548},
        {"title": "Шаг 5 — Бизнес-модель и личный доход владельца", "start": 3898},
        {"title": "Шаг 6 — Мотивация, бонусы и заработная плата", "start": 4219},
        {"title": "Шаг 7 — Эффективная воронка найма", "start": 4768},
        {"title": "Шаг 8 — Технология делегирования", "start": 5162},
        {"title": "Шаг 9 — Метрики и дэшборд", "start": 5552},
        {"title": "Шаг 10 — Оперативное планирование и координация", "start": 5925},
        {"title": "Шаг 11 — Оцифровка бизнес-процессов", "start": 6266},
        {"title": "Шаг 12 — Стратегия следующего уровня", "start": 6461},
        {"title": "Кейс резидента: клиника Arzu Medical", "start": 6704},
        {"title": "Программа Бизнес Бустер — флагманский продукт", "start": 7479},
        {"title": "Как устроена программа в деталях", "start": 7938},
        {"title": "Уровни поддержки резидентов", "start": 8281},
        {"title": "QnA — ответы на вопросы участников", "start": 10384},
    ],
}

_RAW_BONUSES = [
    {
        "id": "b1",
        "icon": "📝",
        "title": "Бонус 1 — Копирайтинг",
        "desc": "Копирайтинг и маркетинг-кит",
        "topics": [
            {"title": "Копирайтинг и маркетинг-кит", "url": "https://youtu.be/AsejKTiYFYM"},
        ],
    },
    {
        "id": "b2",
        "icon": "🎯",
        "title": "Бонус 2 — Стратегия",
        "desc": "Стратегическое планирование",
        "topics": [
            {"title": "Стратегическое планирование", "url": "https://youtu.be/keG9VwXk0yE"},
        ],
    },
    {
        "id": "b3",
        "icon": "🌟",
        "title": "Бонус 3 — Личный бренд",
        "desc": "Личный бренд и сообщества",
        "topics": [
            {"title": "Личный бренд и сообщества", "url": "https://youtu.be/rOscryWi75I"},
        ],
    },
]

_RAW_TOOLS = [
    {
        "id": "t2026",
        "icon": "🛠",
        "title": "Инструменты 2026",
        "desc": "30 практических занятий",
        "playlistUrl": "https://www.youtube.com/playlist?list=PLuHIwD8UzKjyDvo1I992H57blcW6U2vR1",
        "topics": [],
    },
    {
        "id": "t2025",
        "icon": "📋",
        "title": "Инструменты 2025",
        "desc": "88 практических занятий",
        "playlistUrl": "https://www.youtube.com/playlist?list=PLuHIwD8UzKjyDvo1I992H57blcW6U2vR1",
        "topics": [],
    },
]


def _build_days() -> list[dict]:
    days = []
    for raw in _RAW_DAYS:
        topics = _build_topics(raw["topics"])
        _validate_chronology(f"День {raw['id']}", topics)
        days.append(
            {
                "id": raw["id"],
                "title": raw["title"],
                "videoHlsUrl": f"{R2_PUBLIC_URL}/day{raw['id']}/playlist.m3u8",
                "topics": topics,
            }
        )
    return days


def _build_bonuses() -> list[dict]:
    bonuses = []
    for raw in _RAW_BONUSES:
        topics = [
            {"title": t["title"], "videoId": _video_id(t["url"]), "startSeconds": 0, "endSeconds": None}
            for t in raw["topics"]
        ]
        bonuses.append(
            {"id": raw["id"], "icon": raw["icon"], "title": raw["title"], "desc": raw["desc"], "topics": topics}
        )
    return bonuses


def _build_tools() -> list[dict]:
    tools = []
    for raw in _RAW_TOOLS:
        tools.append(
            {
                "id": raw["id"],
                "icon": raw["icon"],
                "title": raw["title"],
                "desc": raw["desc"],
                "playlistUrl": raw["playlistUrl"],
                "topics": [],
            }
        )
    return tools


def _build_roadmap() -> dict:
    raw = _RAW_ROADMAP
    topics = _build_topics(raw["topics"])
    _validate_chronology("Дорожная карта", topics)
    return {
        "id": raw["id"],
        "title": raw["title"],
        "videoId": _video_id(raw["videoUrl"]),
        "topics": topics,
    }


def _build_bos_course() -> dict:
    return {
        "days": _build_days(),
        "bonuses": _build_bonuses(),
        "tools": _build_tools(),
        "roadmap": _build_roadmap(),
    }


COURSES: dict[str, dict] = {
    "bos": _build_bos_course(),
}
