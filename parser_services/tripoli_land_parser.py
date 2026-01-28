from pathlib import Path
from datetime import datetime, timezone
import os

import requests
from bs4 import BeautifulSoup
import pandas as pd

from app.parser_services.base_parser import BaseParser
from app.logger import logger
from app.storage_services import HetznerStorageService

BASE_URL = "https://tripoli.land"

COMPANIES = {
    "nibulon": "Нібулон",
    "kernel": "Кернел",
    "lnz-group": "ЛНЗ Груп",
    "tas-agro": "ТАС АГРО",
    "astarta-kiev": "Астарта-Київ",
    "mhp": "МХП",
    "agroprosperis": "Агропросперіс (NCH)",
    "prodinvest-servis": "Продінвест-Сервіс",
    "prometey": "Прометей",
    "expograin": "Експогрейн",
    "mko-trans-servis": "МКО-Транс-Сервіс",
    "ukrzernoinvest-2013": "Укрзернінвест-2013",
    "ooo-spoteks-treyd": "СПОТЕКС-ТРЕЙД",
    "agromino": "Агроміно",
    "ags-grain": "АГС-ГРЕЙН",
    "greynheven": "Грейнхевен",
    "bruklin-kiev": "Бруклін-Київ",
    "ramburs": "Рамбурс",
    "kortiya-ukraina": "Кортія-Україна",
    "glenport": "Гленпорт",
    "ooo-vvt-grupp": "ВВТ-Груп",
    "almeyda-grup-almeida-group": "Алмейда Груп",
    "ooo-agrotreyd-2022": "Агротрейд 2022",
    "dileks-treyd-dilex-trade": "Ділекс Трейд",
    # Додайте інші компанії за потреби
}

GRAIN_NAME_MAPPING = {
    # Cereals
    "Кукурудза": "Corn",
    "Кукурудза фуражна": "Corn",
    "Кукурудза кремниста": "Corn",
    "Пшениця 1 клас": "Wheat 1st grade",
    "Пшениця 2 клас": "Wheat 2nd grade",
    "Пшениця 3 клас": "Wheat 3rd grade",
    "Пшениця 4 клас": "Wheat 4th grade",
    "Пшениця 5 клас": "Wheat 5th grade",
    "Пшениця фуражна": "Wheat 4th grade",
    "Пшениця неконд.": "Wheat 6th grade",
    "Пшениця тверда": "Durum wheat",
    "Ячмінь": "Barley",
    "Овес": "Oats",
    "Овес голозернистий": "Oats",
    "Рис": "Rice",
    "Жито": "Rye",
    "Тритікале": "Triticale",
    "Просо жовте": "Millet",
    "Просо біле": "Millet",
    "Просо червоне": "Millet",
    "Гречка": "Buckwheat",
    "Полба": "Einkorn wheat",

    # Oilseeds
    "Соняшник": "Sunflower",
    "Соняшник високоолеїновий": "Sunflower high-oleic",
    "Соняшник кондитерський": "Confectionery sunflower seeds",
    "Ріпак": "Rapeseed",
    "Ріпак без гмо": "Rapeseed",

    # Pulses / legumes
    "Горох зелений": "Green peas",
    "Горох жовтий": "Peas",
    "Люпин": "Lupine",
    "Боби": "White kidney beans",
    "Квасоля": "White kidney beans",
    "Нут": "Chick-peas",
    "Сочевиця": "Lentil",
    "Сочевиця зелена": "Lentil",

    # Soy
    "Соя": "Soybeans",
    "Соя без гмо": "Soybeans GMO-free",

    # Meals / cakes / oil / byproducts
    "Шрот соняшниковий": "Sunflower seed meal",
    "Жмих соняшниковий": "Sunflower oil cake",
    "Шрот соєвий": "Soybeen meal",
    "Жмих сої": "Жмих сої",
    "Жмих ріпаку": "rapeseed cake",
    "Шрот ріпаку": "Rapeseed coarse meal",
    "Жмих кукурудзи": "Жмих кукурудзи",
    "Висівки пшениці": "Wheat mill offals",
    "Висівки кукурудзи": "Wheat mill offals",

    # Oils and processed
    "Олія соняшникова": "Sunflower oil",
    "Олія соєва": "Soybean oil",
    "Олія ріпакова": "Rapeseed oil",
    "Олія кукурудзяна": "Corn oil",
    "Борошно": "Wheat flour class 1",
    "Цукор": "Sugar",
    "Переробка сої": "Pererobka soyi",

    # Other crops / miscellaneous
    "Сорго біле": "Sorghum white",
    "Сорго червоне": "Sorghum red",
    "Віка": "Vetch",
    "Гірчиця жовта": "Mustard seeds",
    "Гірчиця біла": "Mustard seeds",
    "Гірчиця чорна": "Mustard seeds",
    "Коріандр": "Coriander",
    "Льон": "Flax",
    "Льон золотий": "Flax",
    "Люцерна": "Lucerne",
    "Очеретянка": "Other",
}

BASE_OUT_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_RESULT_DIR = BASE_OUT_DIR / "parsers_results" / "tripoli_land"
os.makedirs(LOCAL_RESULT_DIR, exist_ok=True)
TIMESTAMP = ' '.join(datetime.now(timezone.utc).isoformat().split('.')[0].split('T'))
FILENAME_RESULT = f"{LOCAL_RESULT_DIR}/tripoli_prices_{TIMESTAMP.replace(':', '-').replace(' ', '_')}.csv"
HETZNER_RESULT_PATH = f"tripoli_prices_{TIMESTAMP.replace(':', '-').replace(' ', '_')}.csv"


class TripoliLandParser(BaseParser):
    def parse(self) -> pd.DataFrame:
        all_data = pd.DataFrame()
        for company_slug, company_name in COMPANIES.items():
            logger.info(f"Parsing prices for company: {company_name} ({company_slug})")
            data_company = self._parse_prices(company_slug, company_name)
            if data_company is not None:
                logger.info(f"Found records: {len(data_company)}")
                logger.info(data_company.head(3))
                all_data = pd.concat([all_data, data_company], ignore_index=True)
            else:
                logger.info(f"Failed to get data for company: {company_name}")
        return all_data
    
    def save_results(
        self, results: pd.DataFrame, 
        filepath: str, 
        file_ext: str = "csv", 
        storage_type: str = "local"
    ) -> None:
        if storage_type == "local":
            if file_ext == "csv":
                results.to_csv(filepath, index=False)
            elif file_ext == "excel":
                results.to_excel(filepath, index=False)
            else:
                raise ValueError(f"Unsupported file extension: {file_ext}")
        elif storage_type == "hetzner":
            temp_filepath = f"/tmp/temp_results.{file_ext}"
            if file_ext == "csv":
                results.to_csv(temp_filepath, index=False)
            elif file_ext == "excel":
                results.to_excel(temp_filepath, index=False)
            else:
                raise ValueError(f"Unsupported file extension: {file_ext}")
            
            hetzner_service = HetznerStorageService()
            hetzner_service.upload_file(
                file_path=temp_filepath,
                object_name=os.path.basename(filepath)
            )
            os.remove(temp_filepath)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")



    def _parse_prices(self, company_slug: str, company_name: str):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        }
        logger.info(f"Fetching data for company: {company_name} ({company_slug})")
        # Формуємо URL для компанії
        url = BASE_URL + f"/ua/companies/{company_slug}"

        try:
            logger.info(f"Requesting URL: {url}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # На сайті зазвичай кілька таблиць (Порти та Елеватори)
            tables = soup.find_all('table')
            
            all_data = []

            for table in tables:
                # Отримуємо заголовки (культури: Пшениця, Кукурудза тощо)
                headers_row = table.find('tr')

                # Перевіряємо тип зберігання за заголовком таблиці
                storage_type = headers_row.th.text.strip() if headers_row and headers_row.th else '' # Порт або Елеватор

                if not storage_type or storage_type not in ['Порт', 'Елеватор']:
                    continue

                columns = [th.text.strip() for th in headers_row.find_all(['th', 'td'])[1:]]  # Пропускаємо першу колонку (Назва/Адреса)
                
                # Обробляємо рядки з даними
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) < 2:
                        continue
                    
                    # First column is combined name/address, others are prices
                    location_info = cells[0]
                    storage_name = location_info.b.text.strip() if location_info.b else ''
                    region = location_info.p.text.strip() if location_info.p else ''
                    entry = {
                        "timestamp": TIMESTAMP,
                        "company": company_name,
                        "storage_type": storage_type,
                        "storage_location": storage_name,
                        "address_region": region
                    }
                    
                    # Додаємо ціни відповідно до заголовків колонок
                    for i in range(1, len(cells)):
                        if i < len(columns):
                            culture_name = columns[i]
                            price_value = cells[i].text.strip()
                            entry[culture_name] = price_value
                    
                    all_data.append(entry)

            # Створюємо таблицю
            df = pd.DataFrame(all_data)

            # Очистка: видаляємо порожні символи та '-' і усуваємо пробіли в назвах колонок
            df = df.replace('-', '')
            df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]

            # Перетворюємо з широкого формату (колонки-культури) у довгий формат
            meta_cols = ["timestamp", "company", "storage_type", "storage_location", "address_region"]
            culture_cols = [c for c in df.columns if c not in meta_cols]

            if culture_cols:
                df_long = df.melt(id_vars=meta_cols, value_vars=culture_cols, var_name='culture_ua', value_name='price')
                # Очищаємо пробіли та порожні значення цін
                df_long['culture_ua'] = df_long['culture_ua'].astype(str).str.strip()
                df_long = df_long[df_long['price'].notna() & (df_long['price'].astype(str) != '')]

                # Мапінг української назви до назви категорії у БД
                df_long['category_name'] = df_long['culture_ua'].map(GRAIN_NAME_MAPPING).fillna(df_long['culture_ua'])

                return df_long.reset_index(drop=True)

            return df

        except Exception as e:
            logger.info(f"Error during parsing: {e}")
            return None

if __name__ == "__main__":
    parser = TripoliLandParser()
    results_df = parser.parse()
    if not results_df.empty:
        parser.save_results(
            results=results_df,
            filepath=HETZNER_RESULT_PATH,
            file_ext="csv",
            storage_type="hetzner"
        )
        logger.info(f"Results saved to file: {HETZNER_RESULT_PATH}")
    else:
        logger.info("No data found to save.")