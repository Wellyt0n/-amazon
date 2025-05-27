import os
import re
import sqlite3
import discord
from discord import Embed
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

# Configuração do sistema de logs
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

# Criar logger para erros
error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)
error_file = os.path.join(log_dir, 'erros.log')
error_handler = RotatingFileHandler(error_file, maxBytes=5*1024*1024, backupCount=5)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
error_logger.addHandler(error_handler)

# Criar logger para notificações
notification_logger = logging.getLogger('notification_logger')
notification_logger.setLevel(logging.INFO)
notification_file = os.path.join(log_dir, 'notificacoes.log')
notification_handler = RotatingFileHandler(notification_file, maxBytes=5*1024*1024, backupCount=5)
notification_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
notification_logger.addHandler(notification_handler)

# Função para registrar erros
def log_error(mensagem, excecao=None):
    if excecao:
        error_logger.error(f"{mensagem}: {str(excecao)}")
    else:
        error_logger.error(mensagem)
    print(f"\033[91m❌ ERRO: {mensagem}\033[0m")

# Função para registrar notificações
def log_notification(canal, titulo, desconto=None, preco=None):
    data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    info = f"Canal: {canal} | Título: {titulo}"
    if desconto:
        info += f" | Desconto: {desconto}%"
    if preco:
        info += f" | Preço: R$ {preco}"
    
    notification_logger.info(info)
    print(f"\033[92m✅ Notificação enviada: {info}\033[0m")

# Suprimir logs do TensorFlow e outros
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # 0 = INFO, 1 = WARNING, 2 = ERROR, 3 = FATAL
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=DeprecationWarning)

# Suprimir mensagens de log do Selenium
import logging as selenium_logging
selenium_logging.getLogger('selenium').setLevel(logging.WARNING)
selenium_logging.getLogger('urllib3').setLevel(logging.WARNING)
import time
import threading
import pathlib
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, urlunparse
from threading import Lock
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import (
    NoSuchElementException, SessionNotCreatedException, TimeoutException, WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager

# Lock para operações de banco de dados
db_lock = Lock()

# Load environment variables
load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
DISCORD_ALIMENTOS_ID = os.getenv('DISCORD_ALIMENTOS_ID')
DISCORD_BEBE_ID = os.getenv('DISCORD_BEBE_ID')  # Canal específico para produtos de bebê
DISCORD_BELEZA_ID = os.getenv('DISCORD_BELEZA_ID')  # Canal específico para produtos de beleza

# Canais para diferentes faixas de desconto (para categorias gerais)
CHANNEL_20_40 = os.getenv('CHANNEL_20_40')  # 20-39% de desconto
CHANNEL_40_70 = os.getenv('CHANNEL_40_70')  # 40-69% de desconto
CHANNEL_70_100 = os.getenv('CHANNEL_70_100')  # 70-100% de desconto

# Canal padrão (não será mais usado, mas mantido para compatibilidade)
DISCORD_CHANNEL_ID = None

# Discord bot setup
# Não precisamos mais do DISCORD_API_BASE com a biblioteca discord.py
# Vamos usar diretamente o DISCORD_TOKEN

# Categorias de Alimentos e Bebidas
ALIMENTOS_CATEGORIAS = {
    "bebidas_alcoolicas": {
        "name": "Bebidas Alcoólicas",
        "url": "https://amzn.to/3FmcM9F",
    },
    "alimentos_enlatados": {
        "name": "Alimentos Enlatados",
        "url": "https://amzn.to/4jckvVI",
    },
    "cafe_cha_bebidas": {
        "name": "Café, Chá e Outras Bebidas",
        "url": "https://amzn.to/4koGgmf",
    },
    "cereal_cafe": {
        "name": "Cereal de Café",
        "url": "https://amzn.to/3Zpl1sp",
    },
    "papinhas_bebe": {
        "name": "Papinhas de Bebê",
        "url": "https://amzn.to/4mm1Idh",
    },
    "ervas_temperos": {
        "name": "Ervas e Temperos",
        "url": "https://amzn.to/3H0FsFX",
    },
    "geleias_mel_pasta": {
        "name": "Geleias, Mel e Pasta",
        "url": "https://amzn.to/4dvfab4",
    },
    "arroz_massas": {
        "name": "Arroz e Massas",
        "url": "https://amzn.to/44JIgB9",
    },
    "ingredientes_culinaria": {
        "name": "Ingredientes para Culinária e Confeitaria",
        "url": "https://amzn.to/3S8Lu9N",
    },
    "lanches_doces": {
        "name": "Lanches e Doces",
        "url": "https://amzn.to/3H16oVQ",
    },
    "laticinios_ovos": {
        "name": "Laticínios, Ovos e Alternativas à Base de Vegetal",
        "url": "https://amzn.to/4j91sf6",
    },
    "molhos_condimentos": {
        "name": "Molhos e Condimentos",
        "url": "https://amzn.to/4kkyNog",
    },
    "oleos_azeites": {
        "name": "Óleos, Azeites, Vinagres e Molhos para Salada",
        "url": "https://amzn.to/3SbHO75",
    },
    "padaria_confeitaria": {
        "name": "Padaria e Confeitaria",
        "url": "https://amzn.to/3SaTsiA",
    },
}

# ───────── Thread-safe database connection ────────────────────────────────────
from threading import Lock

# Lock global para operações no banco de dados
db_lock = Lock()

class ThreadSafeDatabase:
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init_db()
        return cls._instance
    
    def _init_db(self):
        try:
            # Configuração do banco de dados
            self.conn = sqlite3.connect('amazon_products.db', check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute('PRAGMA journal_mode=WAL')  # Melhora concorrência
            self._ensure_tables()
        except Exception as e:
            print(f"Erro ao inicializar banco de dados: {e}")
            raise
        
    def _ensure_tables(self):
        with db_lock:  # Usar o lock global aqui
            cursor = self.conn.cursor()
            # Create products table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                asin TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                current_price REAL NOT NULL,
                url TEXT NOT NULL,
                image_url TEXT,
                category_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # Create price history table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asin TEXT NOT NULL,
                price REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (asin) REFERENCES products(asin) ON DELETE CASCADE
            )''')
            
            # Create blocked products table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS blocked_products (
                asin TEXT PRIMARY KEY,
                blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                blocked_by TEXT,
                reason TEXT DEFAULT 'User reaction'
            )''')
            
            # Create sent notifications table to track discount history
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sent_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asin TEXT NOT NULL,
                discount_percent REAL NOT NULL,
                price REAL NOT NULL,
                reference_price REAL NOT NULL,
                channel_id TEXT NOT NULL,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notification_type TEXT DEFAULT 'normal'
            )''')
            
            # Create index for faster lookups
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_asin ON products(asin)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_asin ON price_history(asin)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_created ON price_history(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_blocked_asin ON blocked_products(asin)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sent_notifications_asin ON sent_notifications(asin)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sent_notifications_sent_at ON sent_notifications(sent_at)')
            
            self.conn.commit()
    
    def get_connection(self):
        return self.conn
    
    def close(self):
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except Exception as e:
                print(f"Erro ao fechar conexão: {e}")
    
    def __del__(self):
        self.close()

def asin_from_link(url):
    """Extrai o ASIN de uma URL da Amazon."""
    if not url:
        return None
        
    # Tenta extrair o ASIN da URL
    patterns = [
        r'/dp/([A-Z0-9]{10})',
        r'/gp/product/([A-Z0-9]{10})',
        r'/dp/([A-Z0-9]{10})/',
        r'/gp/product/([A-Z0-9]{10})/'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    # Se não encontrar com os padrões, tenta extrair da query string
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    
    if 'asin' in query:
        return query['asin'][0]
    
    return None

def get_average_price(conn, asin, days=30):
    """
    Calcula a média de preço de um produto nos últimos N dias.
    Implementação melhorada para maior consistência e confiabilidade.
    """
    if not conn or not asin:
        return None
        
    cursor = conn.cursor()
    
    # Verificar se há registros suficientes para cálculo
    cursor.execute('SELECT COUNT(*) FROM price_history WHERE asin = ?', (asin,))
    count = cursor.fetchone()[0]
    
    if count < 2:  # Precisa de pelo menos 2 registros para uma média significativa
        return None
    
    # Abordagem unificada: usar todos os registros dos últimos N dias
    # Mas limitar a quantidade para evitar distorções por dados muito antigos
    cursor.execute('''
        SELECT AVG(price) 
        FROM (
            SELECT price 
            FROM price_history 
            WHERE asin = ? 
            AND created_at >= datetime('now', ? || ' days')
            ORDER BY created_at DESC
            LIMIT 30  -- Limita a 30 amostras para cálculo
        ) price_samples
    ''', (asin, f'-{days}'))
    
    result = cursor.fetchone()
    avg_price = result[0] if result else None
    
    # Proteção contra valores inválidos
    if avg_price is not None and avg_price <= 0:
        return None
        
    return avg_price

def get_price_history(conn, asin, days=30, max_points=5):
    """
    Retorna o histórico de preços dos últimos N dias.
    Usa amostragem para limitar o número de pontos retornados.
    """
    cursor = conn.cursor()
    
    # Primeiro, obtém a data mais antiga que precisamos
    cursor.execute('''
        SELECT MIN(created_at) 
        FROM price_history 
        WHERE asin = ? 
        AND created_at >= date('now', ? || ' days')
    ''', (asin, f'-{days}'))
    
    start_date = cursor.fetchone()[0]
    if not start_date:
        return []
        
    # Calcula o intervalo entre amostras
    cursor.execute('''
        SELECT COUNT(*) 
        FROM price_history 
        WHERE asin = ? 
        AND created_at >= ?
    ''', (asin, start_date))
    
    total_points = cursor.fetchone()[0]
    step = max(1, total_points // max_points)  # Pula registros para limitar a max_points
    
    # Obtém amostras distribuídas
    cursor.execute(f'''
        WITH numbered AS (
            SELECT 
                price, 
                created_at,
                ROW_NUMBER() OVER (ORDER BY created_at) as rn
            FROM price_history 
            WHERE asin = ? 
            AND created_at >= ?
            ORDER BY created_at
        )
        SELECT date(created_at) as dia, price
        FROM numbered
        WHERE rn % ? = 0 OR rn = 1 OR rn = (SELECT MAX(rn) FROM numbered)
        ORDER BY created_at DESC
    ''', (asin, start_date, step))
    
    return cursor.fetchall()

def format_price_history(history, current_price=None):
    """Formata o histórico de preços para exibição"""
    if not history:
        return "Sem histórico de preços disponível"
        
    lines = []
    history_list = list(history)  # Converte para lista para poder manipular
    
    # Inverte para mostrar do mais antigo para o mais recente
    history_list.reverse()
    
    for i, (dia, preco) in enumerate(history_list):
        # Calcula quantos dias atrás
        data_obj = datetime.strptime(dia, '%Y-%m-%d')
        dias_atras = (datetime.now().date() - data_obj.date()).days
        
        # Formata o preço com R$ e vírgula
        preco_formatado = f"R$ {preco:,.2f}".replace(".", "X").replace(",", ".").replace("X", ",")
        
        # Adiciona a diferença se houver preço atual
        if current_price is not None:
            diferenca = abs(preco - current_price)
            diferenca_formatada = f"R$ {diferenca:,.2f}".replace(".", "X").replace(",", ".").replace("X", ",")
            lines.append(f"🕒 {dias_atras} dias atrás\n{preco_formatado} | {diferenca_formatada} diferença")
        else:
            lines.append(f"🕒 {dias_atras} dias atrás\n{preco_formatado}")
    
    return "\n".join(lines)

def save_product(conn, title, price, url, image_url, category_id=None):
    """
    Salva ou atualiza um produto no banco de dados.
    Função simplificada - a análise de descontos agora é feita em funções separadas.
    """
    asin = asin_from_link(url)
    if not asin or not price or price <= 0:
        return None
        
    cursor = conn.cursor()
    
    try:
        # Verificar se o produto existe
        cursor.execute('SELECT current_price FROM products WHERE asin = ?', (asin,))
        existing = cursor.fetchone()
        
        current_time = datetime.now()
        
        if existing:
            # Atualizar produto existente
            cursor.execute('''
                UPDATE products 
                SET title = ?, current_price = ?, url = ?, image_url = ?, category_id = ?, updated_at = ?
                WHERE asin = ?
            ''', (title, price, url, image_url, category_id, current_time, asin))
            
            print(f"🔄 Produto atualizado: {asin}")
        else:
            # Novo produto
            cursor.execute('''
                INSERT INTO products (asin, title, current_price, url, image_url, category_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (asin, title, price, url, image_url, category_id, current_time, current_time))
            
            # Inserir primeiro registro no histórico
            cursor.execute('''
                INSERT INTO price_history (asin, price, created_at)
                VALUES (?, ?, ?)
            ''', (asin, price, current_time))
            
            print(f"✅ Novo produto adicionado: {asin}")
            
        conn.commit()
        return asin
        
    except Exception as e:
        log_error(f"Erro ao salvar produto {asin}: {e}")
        try:
            conn.rollback()
        except:
            pass
        return None

def create_ml_search_url(title):
    """Cria uma URL de busca no Mercado Livre com base no título do produto"""
    from urllib.parse import quote
    # Remove termos comuns que podem atrapalhar a busca
    clean_title = ' '.join(word for word in title.split() 
                         if word.lower() not in ['com', 'sem', 'para', 'por', 'no', 'na', 'em', 'de', 'do', 'da', 'das', 'dos'])
    search_query = quote(clean_title[:50])  # Limita o tamanho da busca
    return f"https://lista.mercadolivre.com.br/{search_query}"

def test_discord_connection():
    """Testa a conexão com o Discord e retorna True se estiver funcionando"""
    import asyncio
    
    if not DISCORD_TOKEN:
        log_error("Discord: Token não configurado no arquivo .env")
        return False
        
    if not (CHANNEL_20_40 or CHANNEL_40_70 or CHANNEL_70_100 or DISCORD_ALIMENTOS_ID or DISCORD_BEBE_ID or DISCORD_BELEZA_ID):
        log_error("Discord: Nenhum canal configurado no arquivo .env")
        return False
    
    try:
        print("\n🔌 Testando conexão com o Discord...")
        
        # Função assíncrona para testar login
        async def test_login():
            # Criar um cliente Discord com intents corretos
            intents = discord.Intents.default()
            intents.message_content = True
            client = discord.Client(intents=intents)
            
            try:
                # Tentar fazer login completo para testar a conexão
                await client.login(DISCORD_TOKEN)
                
                # Verificar se conseguimos obter informações do usuário do bot
                app_info = await client.application_info()
                print(f"\n✅ Discord: Conectado como {app_info.name} (ID: {app_info.id})")
                
                # Testar acesso a canais configurados
                channels_to_test = []
                if CHANNEL_20_40: channels_to_test.append((CHANNEL_20_40, "Desconto 20-40%"))
                if CHANNEL_40_70: channels_to_test.append((CHANNEL_40_70, "Desconto 40-70%"))
                if CHANNEL_70_100: channels_to_test.append((CHANNEL_70_100, "Desconto 70-100%"))
                if DISCORD_ALIMENTOS_ID: channels_to_test.append((DISCORD_ALIMENTOS_ID, "Alimentos"))
                if DISCORD_BEBE_ID: channels_to_test.append((DISCORD_BEBE_ID, "Bebê"))
                if DISCORD_BELEZA_ID: channels_to_test.append((DISCORD_BELEZA_ID, "Beleza"))
                
                for channel_id, channel_name in channels_to_test:
                    try:
                        channel = await client.fetch_channel(int(channel_id))
                        print(f"✅ Canal '{channel_name}' ({channel_id}) acessível: {channel.name}")
                    except Exception as e:
                        print(f"❌ Erro ao acessar canal '{channel_name}' ({channel_id}): {e}")
                
                return True
            except discord.errors.LoginFailure as e:
                log_error(f"Discord: Erro de autenticação - {str(e)}")
                return False
            except Exception as e:
                log_error(f"Discord: Erro ao verificar token - {str(e)}")
                return False
            finally:
                # Garantir que o cliente seja fechado corretamente
                await client.close()
                
        # Executar a função assíncrona
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(test_login())
            if result:
                print("\n✅ Discord: Token válido! Conexão estabelecida com sucesso!")
            return result
        finally:
            loop.close()
            
    except Exception as e:
        log_error(f"Discord: Erro ao conectar - {str(e)}")
        return False

def send_discord_message(channel_id, title, description, color, url, image_url=None):
    """Função auxiliar para enviar mensagens para o Discord"""
    try:
        # Verifica se o token e o canal estão configurados
        if not DISCORD_TOKEN:
            log_error("Token do Discord não configurado")
            print(f"\n⚠️ ERRO: Token do Discord não configurado. Verifique o arquivo .env")
            return False
            
        if not channel_id:
            log_error("ID do canal do Discord não configurado")
            print(f"\n⚠️ ERRO: ID do canal do Discord não configurado. Verifique o arquivo .env")
            return False
            
        print(f"\n💬 DEBUG: Preparando mensagem para o canal {channel_id}")
        print(f"   - Título: {title[:50]}...")
        print(f"   - Tamanho da descrição: {len(description)} caracteres")
        print(f"   - URL: {url[:50]}...")
            
        # Cria o embed
        embed = Embed(
            title=title[:256],  # Limita o tamanho do título
            description=description[:2048],  # Limita o tamanho da descrição
            color=color,
            url=url
        )
        
        # Adiciona a imagem se existir
        if image_url:
            embed.set_image(url=image_url)
            
        # Adiciona o timestamp
        embed.timestamp = datetime.utcnow()
        
        # Função assíncrona para enviar a mensagem
        async def send_message():
            # Inicializa o cliente do Discord com intents corretos
            intents = discord.Intents.default()
            intents.message_content = True  # Habilita o acesso ao conteúdo das mensagens
            client = discord.Client(intents=intents)
            
            try:
                # Faz login no Discord
                await client.login(DISCORD_TOKEN)
                
                # Busca o canal
                try:
                    channel = await client.fetch_channel(int(channel_id))
                except discord.errors.NotFound:
                    log_error(f"Canal não encontrado: {channel_id}")
                    return False
                except ValueError:
                    log_error(f"ID de canal inválido: {channel_id}")
                    return False
                
                # Envia a mensagem
                message = await channel.send(embed=embed)
                
                # Adiciona a reação ❌ para permitir bloqueio do produto
                await message.add_reaction("❌")
                print(f"✅ Mensagem enviada com reação ❌ para bloquear produto")
                
                print(f"✅ Mensagem enviada com sucesso para o canal {channel_id}")
                print(f"\n💬 DEBUG: Mensagem enviada com sucesso:")
                print(f"   - Canal: {channel_id}")
                print(f"   - Título: {title[:50]}...")
                print(f"   - Timestamp: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
                return True
            except discord.errors.LoginFailure as e:
                log_error(f"Falha de autenticação no Discord: {e}")
                return False
            except discord.errors.HTTPException as e:
                log_error(f"Erro HTTP ao enviar mensagem: {e}")
                return False
            except Exception as e:
                log_error(f"Erro ao enviar mensagem para o Discord: {e}")
                return False
            finally:
                # Garantir que o cliente seja fechado corretamente
                try:
                    await client.close()
                except:
                    pass
        
        # Executa a função assíncrona
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success = loop.run_until_complete(send_message())
            if not success:
                log_error("Falha ao enviar mensagem para o Discord")
            return success
        finally:
            # Garantir que o loop seja fechado corretamente
            loop.close()
            
    except Exception as e:
        log_error(f"Erro ao enviar notificação para o Discord: {e}")
        return False

def send_discord_notification(asin, title, current_price, avg_price, url, image_url, category_id=None, is_accumulated_discount=False, is_additional_drop=False, conn=None):
    """
    Envia notificações para o Discord com base no tipo e magnitude do desconto.
    Implementação melhorada com mais verificações de segurança e mensagens mais claras.
    """
    # Verificar se o título contém "Cabo" e bloquear notificação
    if "Cabo" in title or "cabo" in title:
        print(f"🚫 Notificação bloqueada - produto contém 'Cabo' no título: {title[:50]}...")
        return False
    
    # Truncar título longo para exibição no log
    short_title = title[:50] + "..." if len(title) > 50 else title
    print(f"\n🔔 Verificando envio de notificação para: {short_title}")
    
    # VERIFICAR SE O PRODUTO ESTÁ BLOQUEADO
    if conn and asin:
        if is_product_blocked(conn, asin):
            print(f"🚫 PRODUTO BLOQUEADO - Pulando notificação para {asin} ({short_title})")
            return False
    
    # NOVA VERIFICAÇÃO: Evitar notificações duplicadas recentes
    if conn:
        try:
            cursor = conn.cursor()
            
            # Verificar se já enviamos notificação para este produto nas últimas 2 horas
            cursor.execute('''
                SELECT COUNT(*) FROM price_history 
                WHERE asin = ? 
                AND created_at >= datetime('now', '-2 hours')
            ''', (asin,))
            recent_notifications = cursor.fetchone()[0]
            
            if recent_notifications > 1:  # Se há mais de 1 entrada nas últimas 2 horas
                print(f"⏭️ PULANDO notificação para {asin} - já processado recentemente (últimas 2 horas)")
                return False
                
            # Verificar se o desconto atual é muito similar ao último notificado
            cursor.execute('''
                SELECT price FROM price_history 
                WHERE asin = ? 
                ORDER BY created_at DESC 
                LIMIT 2
            ''', (asin,))
            recent_prices = cursor.fetchall()
            
            if len(recent_prices) >= 2:
                last_price = recent_prices[0][0]
                before_last_price = recent_prices[1][0]
                
                # Se a variação entre os dois últimos preços for muito pequena (<1%), pular
                if abs(last_price - before_last_price) / max(last_price, before_last_price) < 0.01:
                    print(f"⏭️ PULANDO notificação para {asin} - variação de preço muito pequena")
                    return False
                    
        except Exception as e:
            log_error(f"Erro ao verificar duplicatas para {asin}: {e}")
            # Continuar mesmo se a verificação falhar
    
    # Verificar configuração do Discord
    if not DISCORD_TOKEN:
        log_error("Token do Discord não configurado")
        return False
        
    if not (CHANNEL_20_40 or CHANNEL_40_70 or CHANNEL_70_100):
        log_error("Canais do Discord não configurados")
        return False
    
    # Verificar se temos uma conexão válida com o banco de dados
    if conn is None:
        # Tentar obter uma conexão do ThreadSafeDatabase
        try:
            db = ThreadSafeDatabase()
            conn = db.get_connection()
            print("Obtida nova conexão com o banco de dados para notificação")
        except Exception as e:
            log_error(f"Não foi possível obter conexão com o banco de dados: {e}")
            print(f"⚠️ AVISO: Continuando sem conexão com o banco de dados. Algumas funcionalidades podem ser limitadas.")
            # Continuar mesmo sem conexão
    
    # Verificações de segurança para os preços
    if current_price is None or current_price <= 0:
        log_error(f"Preço atual inválido para {short_title}: {current_price}")
        return False
        
    if avg_price is None or avg_price <= 0:
        log_error(f"Preço médio/referência inválido para {short_title}: {avg_price}")
        return False
    
    # Verificar se o desconto é positivo (preço atual menor que o de referência)
    if current_price >= avg_price:
        print(f"⚠️ Não há desconto real: Preço atual R${current_price:.2f} >= Preço referência R${avg_price:.2f}")
        return False
        
    # Calcular o desconto com proteção contra divisão por zero
    try:
        discount_decimal = (avg_price - current_price) / avg_price
        discount_percent = discount_decimal * 100
    except ZeroDivisionError:
        log_error(f"Erro ao calcular desconto: divisão por zero. Preço médio: {avg_price}")
        return False
        
    print(f"📊 Preço atual: R${current_price:.2f}, Referência: R${avg_price:.2f}, Desconto: {discount_percent:.2f}%")
    
    # VERIFICAÇÃO DE DESCONTO MÍNIMO: Todos os canais devem ter pelo menos 20% de desconto
    if discount_percent < 20:
        print(f"❌ Desconto insuficiente: {discount_percent:.2f}% < 20%. Pulando notificação para TODOS os canais.")
        return False
    
    # VERIFICAÇÃO DE MELHORIA DE DESCONTO: Só notificar se desconto melhorou pelo menos 5%
    if conn and not should_send_notification(conn, asin, discount_percent, min_improvement=5.0):
        return False
    
    # PRIMEIRO: Verificar se é uma categoria com canal específico
    discord_channel = None
    channel_name = ""
    color = 0x00ff00  # Verde padrão
    
    if category_id and category_id in ALIMENTOS_CATEGORIAS and DISCORD_ALIMENTOS_ID:
        discord_channel = DISCORD_ALIMENTOS_ID
        channel_name = "Alimentos"
        color = 0x8B4513  # Marrom para alimentos
        print(f"🍽️ Produto de alimentos detectado - enviando para canal específico: {channel_name} (desconto: {discount_percent:.1f}%)")
    elif category_id == 'bebe' and DISCORD_BEBE_ID:
        discord_channel = DISCORD_BEBE_ID
        channel_name = "Bebê"
        color = 0xFFB6C1  # Rosa claro para bebê
        print(f"👶 Produto de bebê detectado - enviando para canal específico: {channel_name} (desconto: {discount_percent:.1f}%)")
    elif category_id and category_id.startswith('beleza_') and DISCORD_BELEZA_ID:
        discord_channel = DISCORD_BELEZA_ID
        channel_name = "Beleza"
        color = 0xFF69B4  # Rosa para beleza
        print(f"💄 Produto de beleza detectado - enviando para canal específico: {channel_name} (desconto: {discount_percent:.1f}%)")
    else:
        # SEGUNDO: Se não for categoria específica, usar canais de porcentagem
        if discount_percent >= 70:
            discord_channel = CHANNEL_70_100
            channel_name = "Desconto 70-100%"
            color = 0x2ecc71  # Verde
        elif discount_percent >= 40:
            discord_channel = CHANNEL_40_70
            channel_name = "Desconto 40-70%"
            color = 0x3498db  # Azul
        elif discount_percent >= 20:
            discord_channel = CHANNEL_20_40
            channel_name = "Desconto 20-40%"
            color = 0xe67e22  # Laranja
        else:
            # Esta condição nunca deve ser atingida devido à verificação anterior
            print(f"❌ Desconto insuficiente: {discount_percent:.2f}% < 20%. Pulando notificação.")
            return False
        
        print(f"📊 Produto sem categoria específica - enviando para canal de desconto: {channel_name}")

    # Verificar se temos um canal válido
    if not discord_channel:
        log_error("Nenhum canal Discord configurado. Pulando notificação.")
        return False
    
    # Se for um desconto acumulado, envia uma mensagem especial
    if is_accumulated_discount:
        # Registrar no log antes de enviar
        log_notification(
            canal=channel_name,
            titulo=title,
            desconto=round(discount_percent),
            preco=current_price
        )
        
        # Criar a URL de busca no Mercado Livre
        ml_search_url = create_ml_search_url(title)
        
        success = send_discord_message(
            channel_id=discord_channel,
            title=f"🔥 DESCONTO ACUMULADO DE {discount_percent:.0f}% 🔥",
            description=f"**{title}**\n\n"
                      f"📉 **Preço mais alto:** R$ {avg_price:.2f}\n"
                      f"💰 **Preço atual:** R$ {current_price:.2f}\n"
                      f"🎨 **Economia de:** R$ {avg_price - current_price:.2f} ({discount_percent:.0f}%)\n\n"
                      f"🔍 **Links:**\n"
                      f"[Ver no Mercado Livre]({ml_search_url}) • [Comprar na Amazon]({url})",
            color=color,
            url=url,
            image_url=image_url
        )
        
        if success:
            # Registrar notificação enviada
            record_sent_notification(conn, asin, discount_percent, current_price, avg_price, discord_channel, 'accumulated')
            print(f"✅ Notificação de desconto acumulado enviada com sucesso para o canal {channel_name}")
            return True
        else:
            log_error(f"Falha ao enviar notificação de desconto acumulado para o canal {channel_name}")
            return False
    # Se for uma queda adicional
    elif is_additional_drop:
        # Registrar no log antes de enviar
        log_notification(
            canal=channel_name,
            titulo=f"NOVA QUEDA: {title}",
            desconto=round(discount_percent),
            preco=current_price
        )
        
        # Criar a URL de busca no Mercado Livre
        ml_search_url = create_ml_search_url(title)
        
        success = send_discord_message(
            channel_id=discord_channel,
            title=title,
            description=f"**📉 Nova queda de preço!**\n\n"
                      f"💸 **Preço anterior:** R$ {avg_price:.2f}\n"
                      f"💰 **Novo preço:** R$ {current_price:.2f}\n"
                      f"🔺 **Queda de:** {discount_percent:.0f}%\n\n"
                      f"🔄 **Total economizado:** R$ {avg_price - current_price:.2f}\n\n"
                      f"🔍 **Links:**\n"
                      f"[Ver no Mercado Livre]({ml_search_url}) • [Comprar na Amazon]({url})",
            color=0x9b59b6,  # Roxo para quedas adicionais
            url=url,
            image_url=image_url
        )
        
        if success:
            # Registrar notificação enviada
            record_sent_notification(conn, asin, discount_percent, current_price, avg_price, discord_channel, 'additional_drop')
            print(f"✅ Notificação de queda adicional enviada com sucesso para o canal {channel_name}")
            return True
        else:
            log_error(f"Falha ao enviar notificação de queda adicional para o canal {channel_name}")
            return False
    
    # Notificação padrão para descontos normais
    # Calcular a diferença percentual de preço
    discount_decimal = (avg_price - current_price) / avg_price
    discount_percent = discount_decimal * 100
    price_diff = -discount_percent  # Manter a mesma lógica de sinal (negativo = desconto)
        
    # Obter nome da categoria para exibição
    category_name = ""
    if category_id:
        if category_id in ALIMENTOS_CATEGORIAS:
            category_name = ALIMENTOS_CATEGORIAS[category_id]["name"]
        elif category_id == 'bebe':
            category_name = "Bebê"
        elif category_id and category_id.startswith('beleza_'):
            category_name = CATEGORIAS_BELEZA.get(category_id, {}).get("name", "Beleza")
    
    # Já calculamos o price_diff anteriormente
    ml_search_url = create_ml_search_url(title)
    
    # Formata o preço para exibição
    price_str = f"R$ {current_price:,.2f}".replace(".", "X").replace(",", ".").replace("X", ",")
    avg_price_str = f"R$ {avg_price:,.2f}".replace(".", "X").replace(",", ".").replace("X", ",")
    
    # Criar um embed usando discord.py
    embed_color = 0x00ff00 if price_diff < 0 else 0xff0000
    
    # Criar a descrição para o embed
    description = f"💰 **Preço Atual:** {price_str}\n"
    description += f"📊 **Média (30 dias):** {avg_price_str}\n"
    description += f"📉 **Diferença:** {price_diff:+.2f}%\n"
    
    if category_name:
        description += f"\n📜 **Categoria:** {category_name}\n"
        
    description += f"\n🔍 **Links:**\n"
    description += f"[Ver no Mercado Livre]({ml_search_url}) • [Comprar na Amazon]({url})"
    
    # Enviar a mensagem usando nossa função que já foi atualizada para usar discord.py
    # Adicionar informações de ASIN e timestamp na descrição
    description += f"\n\n**ASIN:** {asin} • {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    print(f"✅ Enviando notificação para o canal {discord_channel} com {discount_percent:.2f}% de desconto")
    print(f"\n💬 DEBUG: Detalhes da notificação:")
    print(f"   - Canal: {channel_name} (ID: {discord_channel})")
    print(f"   - Título: {title[:50]}...")
    print(f"   - Desconto: {discount_percent:.2f}%")
    print(f"   - URL da imagem: {'Configurada' if image_url else 'Não configurada'}")
    
    # Busca histórico de preços e adiciona na mensagem
    if conn:
        try:
            price_history = get_price_history(conn, asin, days=30, max_points=5)
            if price_history:
                history_text = format_price_history(price_history, current_price)
                description += f"\n\n**Histórico de Preços:**\n{history_text}"
        except Exception as e:
            log_error(f"Erro ao obter histórico de preços: {e}")
            # Continuar mesmo se falhar ao obter o histórico
    
    # Registrar no log antes de enviar a notificação padrão
    log_notification(
        canal=channel_name,
        titulo=title,
        desconto=round(discount_percent),
        preco=current_price
    )
    
    # Enviar a mensagem e verificar se foi enviada com sucesso
    success = send_discord_message(
        channel_id=discord_channel,
        title=title[:256],
        description=description,
        color=embed_color,
        url=url,
        image_url=image_url
    )
    
    if success:
        # Registrar notificação enviada
        record_sent_notification(conn, asin, discount_percent, current_price, avg_price, discord_channel, 'normal')
        print(f"✅ Notificação enviada com sucesso para o canal {channel_name}")
    else:
        log_error(f"Falha ao enviar notificação para o canal {channel_name}")
    
    return success

# ───────── utilidades ────────────────────────────────────────────────────────────
def normalizar_url(link: str, tag="titaniumpro04-20") -> str:
    u = urlparse(link)
    qs = parse_qs(u.query)
    asin = None
    parts = u.path.split("/")
    if "dp" in parts and len(parts) > parts.index("dp") + 1:
        asin = parts[parts.index("dp") + 1]
    if not asin:
        asin = qs.get("asin", [None])[0]
    if asin and re.fullmatch(r"[A-Z0-9]{10}", asin, re.I):
        return urlunparse(u._replace(path=f"/dp/{asin}", query=f"tag={tag}", fragment=""))
    return urlunparse(u._replace(query="", fragment=""))

parse_price = lambda t: float(t.strip().replace("R$", "").replace(".", "").replace(",", ".") or 0)

def extrair(elem):
    try:
        titulo = elem.find_element(By.CSS_SELECTOR, "h2").text.strip()
        preco_txt = elem.find_element(By.CSS_SELECTOR, ".a-price .a-offscreen").get_attribute("textContent")
        preco = parse_price(preco_txt)
        link = normalizar_url(elem.find_element(By.CSS_SELECTOR, "a").get_attribute("href"))
        img = elem.find_element(By.TAG_NAME, "img").get_attribute("src")
        return titulo, preco, link, img
    except Exception:
        return None

def asin_from_link(link: str) -> str:
    if not link:
        return ""
    try:
        # Try to extract ASIN from /dp/ pattern
        if "/dp/" in link:
            return link.split("/dp/")[1].split("/")[0].split("?")[0]
        # Try to extract from /gp/product/ pattern
        elif "/gp/product/" in link:
            return link.split("/gp/product/")[1].split("/")[0].split("?")[0]
        # Try to extract from /product/ pattern
        elif "/product/" in link:
            return link.split("/product/")[1].split("/")[0].split("?")[0]
        # If no known pattern matches, try to find ASIN in the URL
        asin_match = re.search(r'[A-Z0-9]{10}(?:[A-Z0-9])?', link.upper())
        if asin_match:
            return asin_match.group(0)
        return ""
    except (IndexError, AttributeError):
        return ""

# ───────── Selenium otimizado ────────────────────────────────────────────────────
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
CHROMEDRIVER = SCRIPT_DIR / "chromedriver-win32" / "chromedriver.exe"  # Usando o novo ChromeDriver

opts = webdriver.ChromeOptions()
opts.page_load_strategy = "eager"
opts.add_argument("--disable-gpu")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-extensions")
opts.add_argument("--disable-blink-features=AutomationControlled")
opts.add_experimental_option("excludeSwitches", ["enable-logging"])
opts.add_experimental_option("prefs", {
    "profile.managed_default_content_settings.images": 2,
    "profile.managed_default_content_settings.stylesheets": 2,
    "profile.managed_default_content_settings.fonts": 2,
})

# Não inicializar o driver globalmente para evitar conflitos
# O driver será inicializado em cada thread separadamente


# ───────── categorias ────────────────────────────────────────────────────────────
# Categoria de Bebê
CATEGORIA_BEBE = {
    "bebe": {
        "name": "Bebê",
        "url": "https://amzn.to/4j7pwif"
    }
}

# Categorias de Beleza
CATEGORIAS_BELEZA = {
    "beleza_corpo_banho": {
        "name": "Produtos de Corpo e Banho",
        "url": "https://amzn.to/4mn4mjd"
    },
    "beleza_cuidados_cabelo": {
        "name": "Cuidados com o Cabelo",
        "url": "https://amzn.to/4dpOatq"
    },
    "beleza_depilacao_barbear": {
        "name": "Produtos para Depilação e Aparelhos de Barbear",
        "url": "https://amzn.to/3H0Ryil"
    },
    "beleza_manicure_pedicure": {
        "name": "Produtos de Manicure e Pedicure",
        "url": "https://amzn.to/43j5BHh"
    },
    "beleza_maquiagem": {
        "name": "Maquiagem",
        "url": "https://amzn.to/4jgoxwv"
    },
    "beleza_mobiliarios_spas": {
        "name": "Mobiliários de Spas",
        "url": "https://amzn.to/44MgR1k"
    },
    "beleza_cuidados_pele": {
        "name": "Produtos de Cuidados com a Pele",
        "url": "https://amzn.to/43BgpBX"
    },
    "beleza_perfumes": {
        "name": "Perfumes e Fragrâncias",
        "url": "https://amzn.to/4kqXZtm"
    },
    "beleza_utensilios": {
        "name": "Utensílios e Acessórios de Beleza",
        "url": "https://amzn.to/4jarHBE"
    },
    "beleza_saude_cuidados": {
        "name": "Saúde e Cuidados Pessoais",
        "url": "https://amzn.to/4mofrQU"
    }
}

# Categoria de Brinquedos
CATEGORIAS_BRINQUEDOS = {
    "brinquedos": {
        "name": "Brinquedos e Jogos",
        "url": "https://amzn.to/3GXPiYX"
    }
}

# Categoria de Malas
CATEGORIAS_MALAS = {
    "malas_viagem": {
        "name": "Malas e Mochilas de Viagem",
        "url": "https://amzn.to/45gQL6I"
    }
}

# Categorias automotivas
CATEGORIAS_AUTOMOTIVAS = {
    "cuidados_automotivos": {
        "name": "Cuidados Automotivos",
        "url": "https://amzn.to/3H0Q6wp"
    },
    "navegacao_automotiva": {
        "name": "Sistemas de Navegação Automotiva",
        "url": "https://amzn.to/4je8S0p"
    },
    "eletronicos_automotivos": {
        "name": "Eletrônicos e Tecnologia Automotivos",
        "url": "https://amzn.to/43ADqox"
    },
    "ferramentas_automotivas": {
        "name": "Ferramentas e Equipamentos Automotivos",
        "url": "https://amzn.to/43pbtiw"
    },
    "oleos_automotivos": {
        "name": "Óleos e Fluidos Automotivos",
        "url": "https://amzn.to/3SGsTC6"
    },
    "pecas_automotivas": {
        "name": "Peças e Acessórios para Automóveis",
        "url": "https://amzn.to/3SGsTC6"
    },
    "acessorios_motos": {
        "name": "Acessórios e Peças para Motos",
        "url": "https://amzn.to/3FpDtu6"
    },
    "veiculos_agricolas": {
        "name": "Peças e Equipamentos para Veículos Agrícolas",
        "url": "https://amzn.to/43fMkYN"
    },
    "pneus_rodas": {
        "name": "Pneus e Rodas Automotivas",
        "url": "https://amzn.to/4dko1Mo"
    },
    "tintas_automotivas": {
        "name": "Tintas e Primers Automotivos",
        "url": "https://amzn.to/4jZv2Vt"
    }
}

# Categorias de Eletrônicos
CATEGORIAS_ELETRONICOS = {
    "eletronico_geral": {
        "name": "Eletrônicos Gerais",
        "url": "https://amzn.to/43i9Nak"
    },
    "acessorios_foto_video": {
        "name": "Acessórios para Foto e Vídeo para Celulares",
        "url": "https://amzn.to/4mp62bT"
    },
    "acessorios_ugreen": {
        "name": "Acessórios Ugreen",
        "url": "https://amzn.to/4krHGMX"
    },
    "caixas_som_portateis": {
        "name": "Caixas de Som e Bases Portáteis",
        "url": "https://amzn.to/4do1MFD"
    },
    "eletronicos_amazon_br": {
        "name": "Eletrônicos Amazon BR",
        "url": "https://amzn.to/3SHcFIP"
    },
    "smartwatches_eua": {
        "name": "Smartwatches EUA",
        "url": "https://amzn.to/3FedDte"
    },
    "celulares_comunicacao": {
        "name": "Celulares e Comunicação",
        "url": "https://amzn.to/4kM1VV"
    }
}

DEFAULT_CATEGORIES = [
    # Categorias de Eletrônicos
    {
        "name": "Eletrônicos Gerais",
        "url": "https://amzn.to/43i9Nak",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "eletronico"
    },
    # Computadores e Informática
    {
        "name": "Computadores e Informática",
        "url": "https://amzn.to/4kk6OVC",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "informatica"
    },
    # Filtros, Bebedouros e Refrigeradores de Água
    {
        "name": "Filtros e Bebedouros de Água",
        "url": "https://amzn.to/3FoP78B",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "filtros_agua"
    },
    # Eletroportáteis de Cozinha
    {
        "name": "Eletroportáteis de Cozinha",
        "url": "https://amzn.to/3Zl8HcI",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "eletroportateis_cozinha"
    },
    # Stanley
    {
        "name": "Stanley",
        "url": "https://amzn.to/3GY6kpR",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "stanley"
    },
    # Eletrodomésticos
    {
        "name": "Eletrodomésticos",
        "url": "https://amzn.to/4kcfq1e",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "eletrodomesticos"
    },
    # Panelas
    {
        "name": "Panelas",
        "url": "https://amzn.to/3HhvZdb",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "panelas"
    },
    # Eletronicos
    {
        "name": "Eletronicos",
        "url": "https://amzn.to/4jgxV3h",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "eletronicos_gerais"
    },
    # TV, Áudio e Cinema em Casa
    {
        "name": "TV, Áudio e Cinema em Casa",
        "url": "https://amzn.to/4mq5ZMU",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "tv_audio"
    },
    # Produtos para Câmeras e Foto
    {
        "name": "Produtos para Câmeras e Foto",
        "url": "https://amzn.to/43bamEg",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "cameras_foto"
    },
    # Suplementos e Pré-Treino
    {
        "name": "Suplementos e Pré-Treino",
        "url": "https://amzn.to/4mHQvUX",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "suplementos"
    },
    # Eletrônico sportivos
    {
        "name": "Eletrônicos Esportivos",
        "url": "https://amzn.to/4moTdyn",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "eletronicos_esportivos"
    },
    # Ferramentas e Materiais de Construção
    {
        "name": "Ferramentas e Materiais de Construção",
        "url": "https://amzn.to/4kqNDJV",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "ferramentas_construcao"
    },
    # Ferramentas Elétricas
    {
        "name": "Ferramentas Elétricas",
        "url": "https://amzn.to/44OW2T6",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "ferramentas_eletricas"
    },
    # Ferramentas Manuais
    {
        "name": "Ferramentas Manuais",
        "url": "https://amzn.to/3YWqh6F",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "ferramentas_manuais"
    },
    # Organizadores de Ferramentas
    {
        "name": "Organizadores de Ferramentas",
        "url": "https://amzn.to/4ktDn3J",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "organizadores_ferramentas"
    },
    # Ferramentas de Medição
    {
        "name": "Ferramentas de Medição",
        "url": "https://amzn.to/4k2DtPM",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "ferramentas_medicao"
    },
    # Equipamento Elétrico
    {
        "name": "Equipamento Elétrico",
        "url": "https://amzn.to/4jnhzWE",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "equipamento_eletrico"
    },
    # Acessórios de Ferramentas Elétricas
    {
        "name": "Acessórios de Ferramentas Elétricas",
        "url": "https://amzn.to/4mgAnJA",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "acessorios_ferramentas"
    },
    # Proteção e Segurança
    {
        "name": "Proteção e Segurança",
        "url": "https://amzn.to/45q9uNc",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "protecao_seguranca"
    },
    # Chuveiro
    {
        "name": "Chuveiro",
        "url": "https://amzn.to/43nB0IB",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "chuveiro"
    },
    # Games
    {
        "name": "Games",
        "url": "https://amzn.to/3H4Zkrl",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "games"
    },
    # Jogos de Tabuleiro
    {
        "name": "Jogos de Tabuleiro",
        "url": "https://amzn.to/43tSmnq",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "jogos_tabuleiro"
    },
    # PetShop
    {
        "name": "PetShop",
        "url": "https://amzn.to/3SPEC16",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "petshop"
    },
    # Teclados
    {
        "name": "Teclados",
        "url": "https://amzn.to/3H5Sj9W",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "teclados"
    },
    # DJI
    {
        "name": "DJI",
        "url": "https://amzn.to/3YW7p7X",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "dji"
    },
    # Roteador
    {
        "name": "Roteador",
        "url": "https://amzn.to/4mplGnH",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "roteador"
    },
    # Lego
    {
        "name": "Lego",
        "url": "https://amzn.to/3F24TX5",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "lego"
    },
    # Robô Aspirador
    {
        "name": "Robô Aspirador",
        "url": "https://amzn.to/44NxslA",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "robo_aspirador"
    },
    # SSD
    {
        "name": "SSD",
        "url": "https://amzn.to/3Fj23gk",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "ssd"
    },
    # Memórias RAM
    {
        "name": "Memórias RAM",
        "url": "https://amzn.to/4jg5Ys1",
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": "memorias_ram"
    },
]

# Adicionar categorias de alimentos
for cat_id, cat_info in ALIMENTOS_CATEGORIAS.items():
    DEFAULT_CATEGORIES.append({
        "name": cat_info["name"],
        "url": cat_info["url"],
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": cat_id
    })

# Adicionar categorias automotivas
for cat_id, cat_info in CATEGORIAS_AUTOMOTIVAS.items():
    DEFAULT_CATEGORIES.append({
        "name": cat_info["name"],
        "url": cat_info["url"],
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": cat_id
    })

# Adicionar categoria de bebê
for cat_id, cat_info in CATEGORIA_BEBE.items():
    DEFAULT_CATEGORIES.append({
        "name": cat_info["name"],
        "url": cat_info["url"],
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": cat_id
    })

# Adicionar categorias de beleza
for cat_id, cat_info in CATEGORIAS_BELEZA.items():
    DEFAULT_CATEGORIES.append({
        "name": cat_info["name"],
        "url": cat_info["url"],
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": cat_id
    })

# Categorias de Casa e Jardim
CATEGORIAS_CASA_JARDIM = {
    "jardim_piscina": {
        "name": "Jardim e Piscina",
        "url": "https://amzn.to/43BvG5L"
    },
    "produtos_limpeza": {
        "name": "Produtos de Limpeza",
        "url": "https://amzn.to/4doABKJ"
    },
    "produto_banho": {
        "name": "Produto de Banho",
        "url": "https://amzn.to/3YNZ0Dy"
    },
    "cama_mesa_banho": {
        "name": "Cama, Mesa e Banho",
        "url": "https://amzn.to/3S82T2m"
    },
    "cozinha_jantar": {
        "name": "Cozinha e Sala de Jantar",
        "url": "https://amzn.to/4dJbuCJ"
    },
    "decoracao_casa": {
        "name": "Produtos de Decoração para Casa",
        "url": "https://amzn.to/4dzuzqH"
    },
    "passar_roupa": {
        "name": "Produtos para Passar Roupa",
        "url": "https://amzn.to/4moz1wk"
    },
    "iluminacao": {
        "name": "Iluminação",
        "url": "https://amzn.to/4ju1tdT"
    },
    "moveis_decoracao": {
        "name": "Móveis e Decoração",
        "url": "https://amzn.to/43ExDOM"
    },
    "ar_ventilacao": {
        "name": "Ar e Ventilação",
        "url": "https://amzn.to/4kk4TQU"
    }
}

# Adicionar categorias de casa e jardim
for cat_id, cat_info in CATEGORIAS_CASA_JARDIM.items():
    DEFAULT_CATEGORIES.append({
        "name": cat_info["name"],
        "url": cat_info["url"],
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": cat_id
    })

# Adicionar categorias de malas
for cat_id, cat_info in CATEGORIAS_MALAS.items():
    DEFAULT_CATEGORIES.append({
        "name": cat_info["name"],
        "url": cat_info["url"],
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": cat_id
    })

# Adicionar categorias de brinquedos
for cat_id, cat_info in CATEGORIAS_BRINQUEDOS.items():
    DEFAULT_CATEGORIES.append({
        "name": cat_info["name"],
        "url": cat_info["url"],
        "pagination": "a.s-pagination-next",
        "product": [
            "div[data-component-type='s-search-result']",
            ".s-main-slot .s-result-item"
        ],
        "category_id": cat_id
    })

# Adicionar categorias adicionais de eletrônicos
for cat_id, cat_info in CATEGORIAS_ELETRONICOS.items():
    if cat_id != "eletronico_geral":  # Já adicionamos o geral como padrão
        DEFAULT_CATEGORIES.append({
            "name": cat_info["name"],
            "url": cat_info["url"],
            "pagination": "a.s-pagination-next",
            "product": [
                "div[data-component-type='s-search-result']",
                ".s-main-slot .s-result-item"
            ],
            "category_id": "eletronico"  # Todas compartilham a mesma categoria
        })

# ───────── Database setup ───────────────────────────────────────────────────────
def clean_old_price_history(conn, days=30):
    """Remove registros de preço mais antigos que X dias do histórico."""
    try:
        cursor = conn.cursor()
        
        # Contar quantos registros serão removidos
        cursor.execute(
            'SELECT COUNT(*) FROM price_history WHERE created_at < date("now", ? || " days")',
            (f'-{days}',)
        )
        count = cursor.fetchone()[0]
        
        if count > 0:
            # Remover registros antigos
            cursor.execute(
                'DELETE FROM price_history WHERE created_at < date("now", ? || " days")',
                (f'-{days}',)
            )
            print(f"\n🗑️ Limpeza de banco de dados: {count} registros de preço mais antigos que {days} dias foram removidos")
        else:
            print(f"\n✅ Nenhum registro antigo encontrado (>{days} dias)")
        
        # Verificar produtos órfãos (sem histórico de preços)
        cursor.execute('''
            SELECT COUNT(*) FROM products 
            WHERE asin NOT IN (SELECT DISTINCT asin FROM price_history)
        ''')
        orphaned_count = cursor.fetchone()[0]
        
        if orphaned_count > 0:
            print(f"⚠️ Encontrados {orphaned_count} produtos órfãos (sem histórico), mas mantendo para preservar dados")
            # CORREÇÃO: Não remover produtos órfãos automaticamente, pode ser importante
        
        # Commit as operações
        conn.commit()
        
        # CORREÇÃO: Não fazer VACUUM aqui, será feito separadamente se necessário
        print("✅ Limpeza de histórico concluída")
        
    except sqlite3.Error as e:
        print(f"Erro ao limpar registros antigos: {e}")
        try:
            conn.rollback()
        except:
            pass

def clean_old_notifications(conn, days=7):
    """Remove notificações antigas para manter o banco limpo."""
    try:
        cursor = conn.cursor()
        
        # Contar quantas notificações serão removidas
        cursor.execute(
            'SELECT COUNT(*) FROM sent_notifications WHERE sent_at < date("now", ? || " days")',
            (f'-{days}',)
        )
        count = cursor.fetchone()[0]
        
        if count > 0:
            # Remover notificações antigas
            cursor.execute(
                'DELETE FROM sent_notifications WHERE sent_at < date("now", ? || " days")',
                (f'-{days}',)
            )
            conn.commit()
            print(f"🗑️ Removidas {count} notificações antigas (>{days} dias)")
        
    except sqlite3.Error as e:
        print(f"Erro ao limpar notificações antigas: {e}")

def vacuum_database_if_needed(conn):
    """Executa VACUUM no banco apenas se necessário e de forma segura."""
    try:
        # Verificar se é necessário fazer VACUUM (fragmentação > 10%)
        cursor = conn.cursor()
        cursor.execute('PRAGMA page_count')
        page_count = cursor.fetchone()[0]
        
        cursor.execute('PRAGMA freelist_count')
        freelist_count = cursor.fetchone()[0]
        
        if page_count > 0:
            fragmentation = (freelist_count / page_count) * 100
            
            if fragmentation > 10:  # Se fragmentação > 10%
                print(f"🔧 Fragmentação detectada: {fragmentation:.1f}% - executando VACUUM...")
                
                # Fechar conexões existentes e fazer VACUUM em nova conexão
                conn.execute('VACUUM')
                print("✨ VACUUM concluído - banco de dados otimizado")
            else:
                print(f"✅ Banco de dados em bom estado (fragmentação: {fragmentation:.1f}%)")
        
    except sqlite3.Error as e:
        print(f"Aviso: Não foi possível executar VACUUM: {e}")
        # Não é crítico, continua funcionando

def optimize_database(conn):
    """Otimiza o banco de dados removendo duplicatas e reorganizando dados."""
    try:
        cursor = conn.cursor()
        
        # CORREÇÃO: Remover apenas duplicatas REAIS (mesmo ASIN, mesmo preço, mesmo HORA)
        # Não remover registros do mesmo dia com mesmo preço, pois isso é histórico válido
        cursor.execute('''
            DELETE FROM price_history 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM price_history 
                GROUP BY asin, price, datetime(created_at, 'start of hour')
            )
        ''')
        duplicates_removed = cursor.rowcount
        
        if duplicates_removed > 0:
            print(f"🗑️ Removidas {duplicates_removed} duplicatas REAIS do histórico de preços (mesma hora)")
        else:
            print("✅ Nenhuma duplicata real encontrada no histórico de preços")
        
        # Mostrar estatísticas de produtos bloqueados
        cursor.execute('SELECT COUNT(*) FROM blocked_products')
        blocked_count = cursor.fetchone()[0]
        
        if blocked_count > 0:
            print(f"🚫 Produtos bloqueados: {blocked_count}")
            
            # Mostrar últimos produtos bloqueados
            cursor.execute('''
                SELECT asin, blocked_by, blocked_at 
                FROM blocked_products 
                ORDER BY blocked_at DESC 
                LIMIT 5
            ''')
            recent_blocks = cursor.fetchall()
            
            if recent_blocks:
                print("📋 Últimos produtos bloqueados:")
                for asin, blocked_by, blocked_at in recent_blocks:
                    blocked_by_clean = blocked_by if blocked_by else "Sistema"
                    print(f"   • {asin} por {blocked_by_clean} em {blocked_at}")
        
        # Mostrar estatísticas de notificações enviadas
        cursor.execute('SELECT COUNT(*) FROM sent_notifications')
        notifications_count = cursor.fetchone()[0]
        
        if notifications_count > 0:
            print(f"📢 Notificações registradas: {notifications_count}")
            
            # Mostrar últimas notificações
            cursor.execute('''
                SELECT asin, discount_percent, notification_type, sent_at 
                FROM sent_notifications 
                ORDER BY sent_at DESC 
                LIMIT 5
            ''')
            recent_notifications = cursor.fetchall()
            
            if recent_notifications:
                print("📋 Últimas notificações enviadas:")
                for asin, discount, notif_type, sent_at in recent_notifications:
                    print(f"   • {asin}: {discount:.1f}% ({notif_type}) em {sent_at}")
        
        # Limpar notificações antigas (7 dias)
        clean_old_notifications(conn, 7)
        
        # Commit todas as operações antes do VACUUM
        conn.commit()
        
        # CORREÇÃO: Fazer VACUUM fora da transação
        conn.execute('PRAGMA optimize')
        print("📊 Estatísticas do banco de dados atualizadas")
        
    except sqlite3.Error as e:
        print(f"Erro ao otimizar banco de dados: {e}")
        try:
            conn.rollback()
        except:
            pass

def get_blocked_products_summary(conn):
    """Retorna um resumo dos produtos bloqueados."""
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM blocked_products')
        total_blocked = cursor.fetchone()[0]
        
        return {
            'total_blocked': total_blocked,
            'status': 'OK' if total_blocked < 1000 else 'ALTO'
        }
    except Exception as e:
        log_error(f"Erro ao obter resumo de produtos bloqueados: {e}")
        return {'total_blocked': 0, 'status': 'ERRO'}

def init_db():
    """Inicializa o banco de dados e retorna a conexão."""
    db = ThreadSafeDatabase()
    conn = db.get_connection()
    
    # Otimizar banco de dados (removendo apenas duplicatas reais)
    print("\n🔧 Otimizando banco de dados...")
    optimize_database(conn)
    
    # Limpar registros MUITO antigos (mais de 30 dias) - preservando histórico recente
    print("\n🧹 Verificando registros antigos...")
    clean_old_price_history(conn, 30)
    
    # Executar VACUUM apenas se necessário
    print("\n🔍 Verificando necessidade de otimização...")
    vacuum_database_if_needed(conn)
    
    print("\n✅ Inicialização do banco de dados concluída")
    
    return conn

# Inicializar o banco de dados
conn = init_db()

# Carregar todos os ASINs de uma vez para memória
with conn:
    cursor = conn.cursor()
    cursor.execute('SELECT asin FROM products')
    asins_gravados = {row[0] for row in cursor.fetchall()}

# Cache para médias de preço
avg_price_cache = {}

# ───────── scraping ──────────────────────────────────────────────────────────────
def process_category(cat):
    print(f"\n▶ Iniciando processamento da categoria: {cat['name']}")
    
    # Configuração do driver para cada thread
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-webgl')
    options.add_argument('--disable-3d-apis')
    
    # Adicionar configurações para evitar bloqueios e melhorar estabilidade
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Configurar um user-agent comum para evitar detecção
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    
    driver = None
    try:
        # Verificar se o ChromeDriver existe
        if not os.path.exists(CHROMEDRIVER):
            print(f"\n❌ ChromeDriver não encontrado em {CHROMEDRIVER}")
            return
            
        print(f"\n🔧 Iniciando Chrome para categoria {cat['name']} usando {CHROMEDRIVER}")
            
        # Usar o ChromeDriver local
        service = Service(executable_path=str(CHROMEDRIVER))
        try:
            driver = webdriver.Chrome(service=service, options=options)
            # Definir um timeout padrão para todas as operações
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
        except Exception as e:
            print(f"\n❌ Erro ao iniciar ChromeDriver para {cat['name']}: {str(e)}")
            return
        
        # Acessar a URL da categoria com retry
        max_retries = 3
        for retry in range(max_retries):
            try:
                driver.get(cat["url"])
                # Esperar pelo carregamento da página com timeout maior
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, cat["product"][0]))
                )
                break  # Se chegar aqui, a página carregou com sucesso
            except TimeoutException:
                if retry < max_retries - 1:
                    print(f"Timeout ao carregar {cat['name']}. Tentativa {retry+1}/{max_retries}")
                    time.sleep(2)  # Esperar um pouco antes de tentar novamente
                    continue
                else:
                    print(f"Falha ao carregar {cat['name']} após {max_retries} tentativas")
                    return  # Sair da função se todas as tentativas falharem
            except Exception as e:
                print(f"Erro ao acessar URL para {cat['name']}: {str(e)}")
                return
        
        pagina = 1
        novos_produtos = []
        produtos_existentes = []  # Nova lista para produtos existentes
        
        while True:
            try:
                # Rolar a página para carregar itens dinâmicos
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)  # Aumentar um pouco o tempo para garantir carregamento
                
                # Coletar elementos de forma mais eficiente
                elems = set()
                for sel in cat["product"]:
                    try:
                        elements = driver.find_elements(By.CSS_SELECTOR, sel)
                        if elements:
                            elems.update(elements)
                            break
                    except Exception as e:
                        continue
                        
                print(f"{cat['name']} - página {pagina}: {len(elems)} itens")
                
                # Processar produtos em lote
                batch_novos = []
                batch_existentes = []
                for el in elems:
                    try:
                        info = extrair(el)
                        if info:
                            title, price, url, image_url = info
                            asin = asin_from_link(url)
                            if asin:
                                category_id = cat.get("category_id")
                                product_data = (asin, title, price, url, image_url, category_id)
                                
                                with db_lock:
                                    if asin not in asins_gravados:
                                        # Produto novo
                                        batch_novos.append(product_data)
                                        asins_gravados.add(asin)
                                    else:
                                        # Produto existente - também precisa ser verificado para descontos
                                        batch_existentes.append(product_data)
                    except Exception as e:
                        # Silenciar erros menores para não poluir o log
                        continue
                
                # Processar lotes de produtos
                if batch_novos:
                    novos_produtos.extend(batch_novos)
                    print(f"{cat['name']} - {len(batch_novos)} novos produtos encontrados")
                    
                if batch_existentes:
                    produtos_existentes.extend(batch_existentes)
                    print(f"{cat['name']} - {len(batch_existentes)} produtos existentes verificados")
                
                # Verificar próxima página
                try:
                    btn_next = driver.find_element(By.CSS_SELECTOR, cat["pagination"])
                    if btn_next.get_attribute("aria-disabled") == "true":
                        break
                        
                    # Navegar para próxima página com retry
                    driver.execute_script("arguments[0].click();", btn_next)
                    pagina += 1
                    time.sleep(2)  # Aumentar tempo entre páginas para evitar bloqueios
                    
                    # Espera otimizada para o carregamento com retry
                    for retry in range(3):
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, cat["product"][0]))
                            )
                            break
                        except TimeoutException:
                            if retry < 2:
                                print(f"Timeout ao carregar página {pagina}. Tentativa {retry+1}/3")
                                time.sleep(2)
                                continue
                            else:
                                print(f"Falha ao carregar página {pagina}. Encerrando categoria.")
                                raise
                    
                except NoSuchElementException:
                    print(f"{cat['name']} - Fim das páginas (botão não encontrado)")
                    break
                except TimeoutException:
                    print(f"{cat['name']} - Timeout ao navegar para página {pagina}. Encerrando.")
                    break
                except Exception as e:
                    print(f"{cat['name']} - Erro ao navegar: {str(e)[:100]}...")
                    break
            except Exception as e:
                print(f"{cat['name']} - Erro ao processar página {pagina}: {str(e)[:100]}...")
                break
                
        # Processar produtos novos e existentes
        db_connection = ThreadSafeDatabase().get_connection()
        
        if novos_produtos:
            print(f"\n💾 {cat['name']} - {len(novos_produtos)} novos produtos encontrados")
            process_batch_notifications(db_connection, novos_produtos, is_new_products=True)
            
        if produtos_existentes:
            print(f"\n🔄 {cat['name']} - {len(produtos_existentes)} produtos existentes verificados")
            process_existing_products(db_connection, produtos_existentes)
            
    except Exception as e:
        print(f"Erro ao processar categoria {cat['name']}: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Garantir que o driver seja fechado mesmo em caso de erro
        if driver:
            try:
                driver.quit()
            except:
                pass

def process_batch_notifications(conn, produtos, is_new_products=False):
    if not produtos:
        return
    
    # Limitar o tamanho do lote para evitar sobrecarga
    max_batch_size = 500
    total_produtos = len(produtos)
    
    if total_produtos > max_batch_size:
        print(f"\n⚠️ Lote muito grande ({total_produtos} produtos). Processando em sub-lotes de {max_batch_size} produtos...")
        
        # Dividir em sub-lotes
        for i in range(0, total_produtos, max_batch_size):
            sub_lote = produtos[i:i+max_batch_size]
            print(f"\n🔍 Processando sub-lote {i//max_batch_size + 1}/{(total_produtos+max_batch_size-1)//max_batch_size} ({len(sub_lote)} produtos)")
            process_batch_notifications(conn, sub_lote, is_new_products)
            
            # Pequena pausa entre sub-lotes para não sobrecarregar o sistema
            if i + max_batch_size < total_produtos:
                print("Pausa de 3 segundos entre sub-lotes...")
                time.sleep(3)
                
        return
    
    # Para produtos novos, apenas salvar no banco sem calcular descontos
    if is_new_products:
        print(f"\n💾 Salvando {len(produtos)} produtos novos no banco de dados...")
        
        with conn:
            cursor = conn.cursor()
            produtos_salvos = 0
            
            # Inserir todos os produtos novos
            for asin, title, price, url, image_url, category_id in produtos:
                try:
                    current_time = datetime.now()
                    
                    # Inserir produto
                    cursor.execute('''
                        INSERT OR IGNORE INTO products (asin, title, current_price, url, image_url, category_id, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (asin, title, price, url, image_url, category_id, current_time, current_time))
                    
                    # Inserir primeiro registro no histórico
                    cursor.execute('''
                        INSERT INTO price_history (asin, price, created_at)
                        VALUES (?, ?, ?)
                    ''', (asin, price, current_time))
                    
                    produtos_salvos += 1
                    
                except Exception as e:
                    log_error(f"Erro ao salvar produto novo {asin}: {e}")
            
            print(f"✅ {produtos_salvos} produtos novos salvos com sucesso")
            print("ℹ️ Produtos novos não são verificados para desconto (sem histórico suficiente)")
        return
    
    # Para produtos existentes (processo original de verificação de descontos)
    print(f"\n🔍 Analisando {len(produtos)} produtos para possíveis notificações...")
        
    # Lista para armazenar produtos com desconto que precisam de notificação
    produtos_com_desconto = []
    
    print(f"\n🔍 DEBUG: Verificando critérios de desconto para {len(produtos)} produtos...")
    
    # Inicializar cache de preços se não existir
    global avg_price_cache
    if 'avg_price_cache' not in globals():
        avg_price_cache = {}
        
    # Limpar o cache se estiver muito grande para evitar vazamento de memória
    if len(avg_price_cache) > 10000:
        avg_price_cache = {}
        
    with conn:
        cursor = conn.cursor()
        current_time = datetime.now()
        
        # CORREÇÃO: Atualizar/inserir produtos usando UPSERT
        for asin, title, price, url, image_url, category_id in produtos:
            try:
                # Verificar se o produto já existe
                cursor.execute('SELECT current_price, updated_at FROM products WHERE asin = ?', (asin,))
                existing_product = cursor.fetchone()
                
                if existing_product:
                    # Produto existe - atualizar
                    old_price = existing_product[0]
                    cursor.execute('''
                        UPDATE products 
                        SET title = ?, current_price = ?, url = ?, image_url = ?, category_id = ?, updated_at = ?
                        WHERE asin = ?
                    ''', (title, price, url, image_url, category_id, current_time, asin))
                    
                    # Verificar se houve mudança significativa no preço (>=0.01)
                    if abs(old_price - price) >= 0.01:
                        # Inserir no histórico apenas se houve mudança significativa
                        cursor.execute('''
                            INSERT INTO price_history (asin, price, created_at)
                            VALUES (?, ?, ?)
                        ''', (asin, price, current_time))
                        print(f"💰 Preço atualizado para {asin}: R${old_price:.2f} → R${price:.2f}")
                    else:
                        print(f"🔄 Produto atualizado (sem mudança de preço): {asin}")
                else:
                    # Produto novo - inserir
                    cursor.execute('''
                        INSERT INTO products (asin, title, current_price, url, image_url, category_id, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (asin, title, price, url, image_url, category_id, current_time, current_time))
                    
                    # Inserir primeiro registro no histórico
                    cursor.execute('''
                        INSERT INTO price_history (asin, price, created_at)
                        VALUES (?, ?, ?)
                    ''', (asin, price, current_time))
                    print(f"✅ Novo produto adicionado: {asin}")
                    
            except Exception as e:
                log_error(f"Erro ao processar produto {asin}: {e}")
        
        # Verificar promoções e preparar notificações
        for asin, title, price, url, image_url, category_id in produtos:
            try:
                # Obter histórico de preços para verificar descontos acumulados
                cursor.execute('''
                    SELECT price, created_at FROM price_history 
                    WHERE asin = ? 
                    AND created_at >= datetime('now', '-30 days')
                    ORDER BY created_at
                ''', (asin,))
                price_history = cursor.fetchall()
                
                # Verificar se há histórico suficiente para calcular descontos
                if price_history and len(price_history) >= 3:  # Aumentado para 3 para ter mais confiabilidade
                    # Pega o preço mais antigo e o mais recente do histórico
                    oldest_price, oldest_date = price_history[0]
                    last_price, last_date = price_history[-1]
                    
                    # Calcula a variação total desde a primeira entrada
                    if oldest_price > 0:
                        total_change_decimal = (oldest_price - price) / oldest_price
                        total_change_percent = total_change_decimal * 100
                        
                        # Verificar se a variação total é significativa (20% ou mais)
                        if total_change_decimal >= 0.20:
                            # Verificar se já notificamos este produto com desconto similar recentemente
                            cursor.execute('''
                                SELECT COUNT(*) FROM price_history 
                                WHERE asin = ? 
                                AND created_at >= datetime('now', '-1 hours')
                            ''', (asin,))
                            recent_updates = cursor.fetchone()[0]
                            
                            # Só notificar se não houve atualização nas últimas 1 hora (evita spam)
                            if recent_updates <= 1:
                                print(f"🔥🔥🔥 DESCONTO ACUMULADO DE {total_change_percent:.1f}% para {asin} (de R${oldest_price:.2f} para R${price:.2f})")
                                produtos_com_desconto.append((asin, title, price, oldest_price, url, image_url, category_id, True, False))
                                continue  # Pular para o próximo produto
                    
                    # Calcula a variação desde a última entrada
                    if len(price_history) >= 2:
                        second_last_price = price_history[-2][0]  # Penúltimo preço
                        if second_last_price > 0:
                            last_change_decimal = (second_last_price - price) / second_last_price
                            last_change_percent = last_change_decimal * 100
                            
                            # Verificar se houve uma queda adicional de 10% desde a penúltima entrada
                            if last_change_decimal >= 0.10:
                                # Verificar se já notificamos recentemente
                                cursor.execute('''
                                    SELECT COUNT(*) FROM price_history 
                                    WHERE asin = ? 
                                    AND created_at >= datetime('now', '-1 hours')
                                ''', (asin,))
                                recent_updates = cursor.fetchone()[0]
                                
                                if recent_updates <= 1:
                                    print(f"📉 QUEDA ADICIONAL DE {last_change_percent:.1f}% para {asin} (de R${second_last_price:.2f} para R${price:.2f})")
                                    produtos_com_desconto.append((asin, title, price, second_last_price, url, image_url, category_id, False, True))
                                    continue  # Pular para o próximo produto
                
                # Calcular média de preço para descontos normais usando o histórico real
                cursor.execute('''
                    SELECT price, created_at 
                    FROM price_history 
                    WHERE asin = ? 
                    ORDER BY created_at
                ''', (asin,))
                price_history = cursor.fetchall()
                
                if price_history and len(price_history) >= 2:
                    # Calcular o preço máximo histórico para comparar com o atual
                    prices = [record[0] for record in price_history]
                    max_price = max(prices)
                    
                    # Usar o preço máximo como referência para o desconto
                    avg_price = max_price
                    
                    # Verificar se a diferença é significativa (pelo menos 20%)
                    if avg_price > 0 and price > 0 and ((avg_price - price) / avg_price) >= 0.20:
                        # Verificar se já notificamos este desconto recentemente
                        cursor.execute('''
                            SELECT COUNT(*) FROM price_history 
                            WHERE asin = ? 
                            AND created_at >= datetime('now', '-2 hours')
                        ''', (asin,))
                        recent_updates = cursor.fetchone()[0]
                        
                        if recent_updates <= 1:
                            print(f"💸 Preço máximo histórico para {asin}: R${avg_price:.2f} (atual: R${price:.2f})")
                        else:
                            # Produto já foi notificado recentemente, pular
                            print(f"⏭️ Produto {asin} já notificado recentemente, pulando...")
                            avg_price = None
                    else:
                        # Se a diferença for menor que 20%, não considerar como desconto
                        avg_price = None
                        print(f"⚠️ Desconto insuficiente para {asin}: {((max_price - price) / max_price) * 100:.1f}% < 20% (R${price:.2f} vs R${max_price:.2f})")
                else:
                    # Não há histórico suficiente para calcular desconto
                    avg_price = None
                    print(f"ℹ️ Produto {asin} sem histórico de preço suficiente para calcular desconto")
                
                # Atualizar o cache
                avg_price_cache[asin] = avg_price
                
                # Verificar se há desconto significativo em relação à média
                if avg_price and avg_price > 0 and price > 0:
                    discount_decimal = (avg_price - price) / avg_price
                    discount_percent = discount_decimal * 100
                    print(f"\n💳 DEBUG: Produto {asin} - Preço atual: R${price:.2f}, Média: R${avg_price:.2f}, Desconto: {discount_percent:.1f}%")
                    if discount_decimal >= 0.20:  # 20% ou mais de desconto
                        # Verificar se já notificamos recentemente para evitar spam
                        cursor.execute('''
                            SELECT COUNT(*) FROM price_history 
                            WHERE asin = ? 
                            AND created_at >= datetime('now', '-2 hours')
                        ''', (asin,))
                        recent_updates = cursor.fetchone()[0]
                        
                        if recent_updates <= 1:
                            print(f"💰 DESCONTO DE {discount_percent:.1f}% em relação à média! {asin} (R${price:.2f} vs média R${avg_price:.2f})")
                            produtos_com_desconto.append((asin, title, price, avg_price, url, image_url, category_id, False, False))
                        else:
                            print(f"⏭️ Produto {asin} já processado recentemente, pulando notificação")
                    else:
                        print(f"❌ Desconto insuficiente: {discount_percent:.1f}% < 20%. Pulando notificação.")
                else:
                    print(f"⚠️ Preço médio indisponível ou inválido para {asin}: {avg_price}")

            except Exception as e:
                log_error(f"Erro ao verificar promoções para {asin}: {e}")
    
    # Enviar notificações em lote para evitar abrir/fechar muitas conexões Discord
    if produtos_com_desconto:
        print(f"\n🔔 Encontrados {len(produtos_com_desconto)} produtos com desconto significativo!")
        print(f"\n🔔 Enviando {len(produtos_com_desconto)} notificações de desconto")
        
        # Mostrar detalhes dos produtos com desconto para depuração
        for i, produto in enumerate(produtos_com_desconto):
            asin, title, price, avg_price, url, image_url, category_id, is_accumulated, is_additional = produto
            discount = ((avg_price - price) / avg_price) * 100 if avg_price > 0 else 0
            print(f"\n📌 Produto {i+1}: {title[:50]}...")
            print(f"   - ASIN: {asin}")
            print(f"   - Preço: R${price:.2f} (média: R${avg_price:.2f})")
            print(f"   - Desconto: {discount:.1f}%")
            print(f"   - Categoria: {category_id if category_id else 'N/A'}")
            print(f"   - Tipo: {'Acumulado' if is_accumulated else 'Adicional' if is_additional else 'Normal'}")
        
        try:
            # Processar em pequenos lotes para evitar sobrecarga
            batch_size = 5
            notificacoes_enviadas = 0
            
            for i in range(0, len(produtos_com_desconto), batch_size):
                batch = produtos_com_desconto[i:i+batch_size]
                for produto in batch:
                    try:
                        if len(produto) == 9:  # Formato com flags de desconto
                            asin, title, price, avg_price, url, image_url, category_id, is_accumulated, is_additional = produto
                            success = send_discord_notification(
                                asin=asin, 
                                title=title, 
                                current_price=price, 
                                avg_price=avg_price, 
                                url=url, 
                                image_url=image_url, 
                                category_id=category_id,
                                is_accumulated_discount=is_accumulated,
                                is_additional_drop=is_additional,
                                conn=conn
                            )
                        else:  # Formato padrão
                            asin, title, price, avg_price, url, image_url, category_id = produto
                            success = send_discord_notification(
                                asin=asin, 
                                title=title, 
                                current_price=price, 
                                avg_price=avg_price, 
                                url=url, 
                                image_url=image_url, 
                                category_id=category_id,
                                conn=conn
                            )
                            
                        if success:
                            notificacoes_enviadas += 1
                    except Exception as e:
                        log_error(f"Erro ao enviar notificação para {asin}: {e}")
                
                # Pequena pausa entre lotes para evitar rate limiting
                if i + batch_size < len(produtos_com_desconto):
                    time.sleep(2)  # Aumentado para 2 segundos para evitar rate limits
            
            print(f"\n✅ {notificacoes_enviadas} notificações enviadas com sucesso de {len(produtos_com_desconto)} tentativas")
                    
        except Exception as e:
            print(f"Erro ao processar lote de notificações: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("\n❌ Nenhum produto com desconto significativo encontrado neste lote.")

def process_categories_parallel(categories, max_threads=3):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    print(f"\n🚀 Iniciando processamento paralelo para {len(categories)} categorias com {max_threads} threads...")
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Enviar todas as categorias para processamento
        future_to_cat = {
            executor.submit(process_category, cat): cat['name'] 
            for cat in categories
        }
        
        # Processar resultados conforme forem concluídos
        for future in as_completed(future_to_cat):
            cat_name = future_to_cat[future]
            try:
                future.result()
                print(f"✅ Categoria concluída: {cat_name}")
            except Exception as e:
                print(f"❌ Erro ao processar categoria {cat_name}: {str(e)}")
    
    end_time = time.time()
    print(f"\n🏁 Todas as categorias foram processadas em {end_time - start_time:.2f} segundos")

# Separar categorias de alimentos
categorias_alimentos = [
    cat for cat in DEFAULT_CATEGORIES 
    if cat.get('category_id') in ALIMENTOS_CATEGORIAS
]

def process_existing_products(conn, produtos):
    """
    Verifica produtos existentes para possíveis descontos.
    Esta função analisa mudanças de preço em produtos que já estão no banco de dados.
    """
    if not produtos:
        return
    
    print(f"\n🔄 Analisando {len(produtos)} produtos existentes para mudanças de preço...")
    
    produtos_com_desconto = []
    produtos_atualizados = 0
    current_time = datetime.now()
    
    with conn:
        cursor = conn.cursor()
        
        for asin, title, current_price, url, image_url, category_id in produtos:
            try:
                # Verificar se há mudança significativa de preço
                cursor.execute('SELECT current_price, updated_at FROM products WHERE asin = ?', (asin,))
                existing_product = cursor.fetchone()
                
                if not existing_product:
                    continue  # Produto não existe no banco (não deveria acontecer)
                
                db_price = existing_product[0]
                last_updated = existing_product[1]
                
                # Verificar se houve mudança significativa no preço (>=1% ou >= R$ 0.10)
                if db_price and db_price > 0:
                    price_change_percent = abs((current_price - db_price) / db_price) * 100
                    price_change_amount = abs(current_price - db_price)
                    
                    if price_change_percent >= 1.0 or price_change_amount >= 0.10:  # Mudança de pelo menos 1% ou R$ 0.10
                        print(f"💱 Mudança de preço detectada para {asin}: R${db_price:.2f} → R${current_price:.2f} ({price_change_percent:+.1f}%)")
                        
                        # Verificar se não atualizamos este produto muito recentemente (últimas 30 minutos)
                        cursor.execute('''
                            SELECT COUNT(*) FROM price_history 
                            WHERE asin = ? 
                            AND created_at >= datetime('now', '-30 minutes')
                        ''', (asin,))
                        recent_updates = cursor.fetchone()[0]
                        
                        if recent_updates == 0:  # Só atualizar se não houve atualizações recentes
                            # Atualizar o produto no banco
                            cursor.execute('''
                                UPDATE products 
                                SET title = ?, current_price = ?, url = ?, image_url = ?, category_id = ?, updated_at = ?
                                WHERE asin = ?
                            ''', (title, current_price, url, image_url, category_id, current_time, asin))
                            
                            # Registrar no histórico de preços
                            cursor.execute('''
                                INSERT INTO price_history (asin, price, created_at)
                                VALUES (?, ?, ?)
                            ''', (asin, current_price, current_time))
                            
                            produtos_atualizados += 1
                            
                            # Verificar se resulta em desconto significativo
                            # Obter histórico de preços para análise
                            cursor.execute('''
                                SELECT price, created_at FROM price_history 
                                WHERE asin = ? 
                                AND created_at >= datetime('now', '-30 days')
                                ORDER BY created_at
                            ''', (asin,))
                            
                            price_history = cursor.fetchall()
                            
                            if price_history and len(price_history) >= 3:  # Histórico suficiente
                                # Verificar desconto acumulado
                                oldest_price = price_history[0][0]
                                if oldest_price > 0:
                                    total_discount = ((oldest_price - current_price) / oldest_price) * 100
                                    if total_discount >= 20:  # Desconto acumulado de 20% ou mais
                                        print(f"🔥 DESCONTO ACUMULADO de {total_discount:.1f}% detectado para produto existente {asin}")
                                        produtos_com_desconto.append((asin, title, current_price, oldest_price, url, image_url, category_id, True, False))
                                        continue
                                
                                # Verificar queda recente
                                if len(price_history) >= 2:
                                    last_price = price_history[-2][0]  # Penúltimo preço
                                    if last_price > 0:
                                        recent_drop = ((last_price - current_price) / last_price) * 100
                                        if recent_drop >= 10:  # Queda recente de 10% ou mais
                                            print(f"📉 QUEDA RECENTE de {recent_drop:.1f}% detectada para produto existente {asin}")
                                            produtos_com_desconto.append((asin, title, current_price, last_price, url, image_url, category_id, False, True))
                                            continue
                            
                            # Verificar desconto em relação à média/máximo histórico
                            if price_history:
                                prices = [record[0] for record in price_history]
                                max_price = max(prices)
                                
                                if max_price > 0:
                                    discount_from_max = ((max_price - current_price) / max_price) * 100
                                    if discount_from_max >= 20:  # Desconto de 20% ou mais em relação ao máximo
                                        print(f"💰 DESCONTO de {discount_from_max:.1f}% em relação ao preço máximo para produto existente {asin}")
                                        produtos_com_desconto.append((asin, title, current_price, max_price, url, image_url, category_id, False, False))
                        else:
                            print(f"⏭️ Produto {asin} já atualizado recentemente (últimos 30 min), pulando...")
                    else:
                        # Sem mudança significativa, apenas atualizar dados básicos sem inserir no histórico
                        # Mas só se não foi atualizado muito recentemente
                        if last_updated:
                            last_update_time = datetime.fromisoformat(last_updated.replace('Z', '+00:00')) if 'Z' in last_updated else datetime.fromisoformat(last_updated)
                            time_diff = current_time - last_update_time
                            
                            # Só atualizar se passou mais de 1 hora desde a última atualização
                            if time_diff.total_seconds() > 3600:  # 1 hora
                                cursor.execute('''
                                    UPDATE products 
                                    SET title = ?, url = ?, image_url = ?, category_id = ?, updated_at = ?
                                    WHERE asin = ?
                                ''', (title, url, image_url, category_id, current_time, asin))
                                print(f"🔄 Dados básicos atualizados para {asin} (sem mudança de preço)")
                
            except Exception as e:
                log_error(f"Erro ao processar produto existente {asin}: {e}")
    
    print(f"\n📊 Produtos existentes processados: {produtos_atualizados} atualizados com mudança de preço")
    
    # Enviar notificações para produtos com desconto
    if produtos_com_desconto:
        print(f"\n🔔 {len(produtos_com_desconto)} produtos existentes com desconto significativo encontrados!")
        
        notificacoes_enviadas = 0
        for produto in produtos_com_desconto:
            try:
                asin, title, price, ref_price, url, image_url, category_id, is_accumulated, is_additional = produto
                
                # Verificar novamente se não enviamos notificação recente para este produto
                with conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT COUNT(*) FROM price_history 
                        WHERE asin = ? 
                        AND created_at >= datetime('now', '-1 hours')
                    ''', (asin,))
                    recent_notifications = cursor.fetchone()[0]
                    
                    # Só enviar se não houve notificação na última hora
                    if recent_notifications <= 1:
                        success = send_discord_notification(
                            asin=asin,
                            title=title,
                            current_price=price,
                            avg_price=ref_price,
                            url=url,
                            image_url=image_url,
                            category_id=category_id,
                            is_accumulated_discount=is_accumulated,
                            is_additional_drop=is_additional,
                            conn=conn
                        )
                        
                        if success:
                            notificacoes_enviadas += 1
                    else:
                        print(f"⏭️ Notificação para {asin} pulada (enviada recentemente)")
                        
                # Pausa entre notificações para evitar rate limiting
                time.sleep(1)
                
            except Exception as e:
                log_error(f"Erro ao enviar notificação para produto existente {asin}: {e}")
        
        print(f"\n✅ {notificacoes_enviadas} notificações enviadas para produtos existentes")
    else:
        print("\n❌ Nenhum desconto significativo encontrado em produtos existentes.")

def block_product(conn, asin, blocked_by=None, reason="User reaction"):
    """Bloqueia um produto para não receber mais notificações."""
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO blocked_products (asin, blocked_by, reason, blocked_at)
                VALUES (?, ?, ?, ?)
            ''', (asin, blocked_by, reason, datetime.now()))
            
            if cursor.rowcount > 0:
                print(f"🚫 Produto {asin} bloqueado com sucesso. Motivo: {reason}")
                return True
            else:
                print(f"⚠️ Produto {asin} já estava bloqueado")
                return False
    except Exception as e:
        log_error(f"Erro ao bloquear produto {asin}: {e}")
        return False

def unblock_product(conn, asin):
    """Desbloqueia um produto para voltar a receber notificações."""
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM blocked_products WHERE asin = ?', (asin,))
            
            if cursor.rowcount > 0:
                print(f"✅ Produto {asin} desbloqueado com sucesso")
                return True
            else:
                print(f"⚠️ Produto {asin} não estava bloqueado")
                return False
    except Exception as e:
        log_error(f"Erro ao desbloquear produto {asin}: {e}")
        return False

def is_product_blocked(conn, asin):
    """Verifica se um produto está bloqueado."""
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM blocked_products WHERE asin = ?', (asin,))
        result = cursor.fetchone()
        return result[0] > 0 if result else False
    except Exception as e:
        log_error(f"Erro ao verificar se produto {asin} está bloqueado: {e}")
        return False

def extract_asin_from_discord_message(content):
    """Extrai ASIN de uma mensagem do Discord."""
    # Procurar padrão "ASIN: XXXXXXXXXX"
    asin_match = re.search(r'ASIN:\s*([A-Z0-9]{10})', content)
    if asin_match:
        return asin_match.group(1)
    
    # Procurar URLs da Amazon na mensagem
    amazon_url_match = re.search(r'https://(?:www\.)?amazon\.com\.br/[^\s]*', content)
    if amazon_url_match:
        return asin_from_link(amazon_url_match.group(0))
    
    return None

def should_send_notification(conn, asin, current_discount_percent, min_improvement=5.0):
    """
    Verifica se deve enviar notificação baseado no histórico de notificações.
    Só envia se:
    1. É a primeira notificação para este produto, OU
    2. O desconto atual é pelo menos 'min_improvement'% maior que a última notificação
    """
    try:
        cursor = conn.cursor()
        
        # Buscar a última notificação enviada para este produto
        cursor.execute('''
            SELECT discount_percent, sent_at 
            FROM sent_notifications 
            WHERE asin = ? 
            ORDER BY sent_at DESC 
            LIMIT 1
        ''', (asin,))
        
        last_notification = cursor.fetchone()
        
        if not last_notification:
            # Primeira notificação para este produto
            print(f"✅ Primeira notificação para {asin} (desconto: {current_discount_percent:.1f}%)")
            return True
        
        last_discount = last_notification[0]
        last_sent_at = last_notification[1]
        
        # Calcular a melhoria do desconto
        discount_improvement = current_discount_percent - last_discount
        
        print(f"📊 {asin}: Desconto atual {current_discount_percent:.1f}% vs último {last_discount:.1f}% (melhoria: {discount_improvement:.1f}%)")
        
        if discount_improvement >= min_improvement:
            print(f"✅ Melhoria suficiente de {discount_improvement:.1f}% >= {min_improvement}% - enviando notificação")
            return True
        else:
            print(f"❌ Melhoria insuficiente de {discount_improvement:.1f}% < {min_improvement}% - pulando notificação")
            return False
            
    except Exception as e:
        log_error(f"Erro ao verificar histórico de notificações para {asin}: {e}")
        # Em caso de erro, permite a notificação para não bloquear o sistema
        return True

def record_sent_notification(conn, asin, discount_percent, price, reference_price, channel_id, notification_type='normal'):
    """Registra uma notificação enviada no banco de dados."""
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sent_notifications 
            (asin, discount_percent, price, reference_price, channel_id, notification_type, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (asin, discount_percent, price, reference_price, channel_id, notification_type, datetime.now()))
        
        conn.commit()
        print(f"📝 Notificação registrada: {asin} com {discount_percent:.1f}% de desconto")
        return True
        
    except Exception as e:
        log_error(f"Erro ao registrar notificação enviada para {asin}: {e}")
        return False

def start_discord_reaction_bot():
    """Inicia um bot Discord para escutar reações e bloquear produtos."""
    import asyncio
    import threading
    
    def run_bot():
        async def main():
            # Configurar intents
            intents = discord.Intents.default()
            intents.message_content = True
            intents.reactions = True
            
            # Criar cliente
            client = discord.Client(intents=intents)
            
            @client.event
            async def on_ready():
                print(f'🤖 Bot de reações iniciado como {client.user}')
                print(f'🔍 Monitorando reações ❌ para bloquear produtos...')
                print(f'📍 Canais monitorados: {[CHANNEL_20_40, CHANNEL_40_70, CHANNEL_70_100, DISCORD_ALIMENTOS_ID, DISCORD_BEBE_ID, DISCORD_BELEZA_ID]}')
            
            @client.event
            async def on_reaction_add(reaction, user):
                try:
                    # Ignorar reações do próprio bot
                    if user == client.user:
                        return
                    
                    # Só processar reações ❌
                    if str(reaction.emoji) != "❌":
                        return
                    
                    print(f"🔍 Reação ❌ detectada de {user.name} na mensagem {reaction.message.id} (canal: {reaction.message.channel.id})")
                    
                    # VERIFICAÇÃO 0: Só processar em canais de notificação configurados
                    valid_channels = []
                    if CHANNEL_20_40: valid_channels.append(int(CHANNEL_20_40))
                    if CHANNEL_40_70: valid_channels.append(int(CHANNEL_40_70))
                    if CHANNEL_70_100: valid_channels.append(int(CHANNEL_70_100))
                    if DISCORD_ALIMENTOS_ID: valid_channels.append(int(DISCORD_ALIMENTOS_ID))
                    if DISCORD_BEBE_ID: valid_channels.append(int(DISCORD_BEBE_ID))
                    if DISCORD_BELEZA_ID: valid_channels.append(int(DISCORD_BELEZA_ID))
                    
                    if valid_channels and reaction.message.channel.id not in valid_channels:
                        print(f"⏭️ Ignorando reação - canal {reaction.message.channel.id} não é canal de notificação")
                        return
                    
                    # VERIFICAÇÃO 1: A mensagem deve ser do próprio bot
                    if reaction.message.author != client.user:
                        print(f"⏭️ Ignorando reação - mensagem não é do bot ({reaction.message.author})")
                        return
                    
                    # VERIFICAÇÃO 2: Deve ter embed (notificações são embeds)
                    if not reaction.message.embeds:
                        print(f"⏭️ Ignorando reação - mensagem sem embed")
                        return
                    
                    embed = reaction.message.embeds[0]
                    
                    # VERIFICAÇÃO 3: Embed deve ter descrição com ASIN
                    if not embed.description:
                        print(f"⏭️ Ignorando reação - embed sem descrição")
                        return
                    
                    # VERIFICAÇÃO 4: Verificar se é realmente uma notificação de produto
                    # Procurar por indicadores de notificação de produto
                    is_product_notification = any([
                        "Preço Atual:" in embed.description,
                        "ASIN:" in embed.description,
                        "Comprar na Amazon" in embed.description,
                        "Ver no Mercado Livre" in embed.description
                    ])
                    
                    if not is_product_notification:
                        print(f"⏭️ Ignorando reação - não é uma notificação de produto")
                        return
                    
                    # Extrair ASIN da descrição do embed
                    asin = extract_asin_from_discord_message(embed.description)
                    
                    if asin:
                        print(f"✅ Reação válida detectada para produto {asin}")
                        
                        # Conectar ao banco e bloquear produto
                        try:
                            db = ThreadSafeDatabase()
                            conn = db.get_connection()
                            
                            success = block_product(
                                conn, 
                                asin, 
                                blocked_by=str(user), 
                                reason=f"Bloqueado via reação ❌ por {user.name}"
                            )
                            
                            if success:
                                # Enviar mensagem de confirmação
                                await reaction.message.reply(
                                    f"🚫 **Produto bloqueado!**\n"
                                    f"📦 ASIN: `{asin}`\n"
                                    f"👤 Bloqueado por: {user.mention}\n"
                                    f"ℹ️ Este produto não receberá mais notificações.\n"
                                    f"💡 Para desbloquear, use: `/desbloquear {asin}`",
                                    delete_after=30
                                )
                                
                                # Adicionar reação de confirmação
                                await reaction.message.add_reaction("✅")
                                
                                print(f"✅ Produto {asin} bloqueado com sucesso por {user.name}")
                            else:
                                await reaction.message.reply(
                                    f"⚠️ Produto `{asin}` já estava bloqueado.",
                                    delete_after=10
                                )
                                
                        except Exception as e:
                            log_error(f"Erro ao bloquear produto via reação: {e}")
                            await reaction.message.reply(
                                f"❌ Erro ao bloquear produto. Verifique os logs.",
                                delete_after=10
                            )
                    else:
                        print(f"⚠️ ASIN não encontrado na notificação {reaction.message.id}")
                        # Não enviar mensagem de erro para não confundir usuários
                    
                except Exception as e:
                    log_error(f"Erro ao processar reação: {e}")
                
                # Comando para desbloquear produtos (funcionalidade extra)
                @client.event
                async def on_message(message):
                    try:
                        # Ignorar mensagens do próprio bot
                        if message.author == client.user:
                            return
                        
                        # Verificar comando de desbloqueio
                        if message.content.startswith('/desbloquear '):
                            asin = message.content.replace('/desbloquear ', '').strip().upper()
                            
                            if re.match(r'^[A-Z0-9]{10}$', asin):
                                try:
                                    db = ThreadSafeDatabase()
                                    conn = db.get_connection()
                                    
                                    success = unblock_product(conn, asin)
                                    
                                    if success:
                                        await message.reply(
                                            f"✅ **Produto desbloqueado!**\n"
                                            f"📦 ASIN: `{asin}`\n"
                                            f"👤 Desbloqueado por: {message.author.mention}\n"
                                            f"ℹ️ Este produto voltará a receber notificações.",
                                            delete_after=20
                                        )
                                    else:
                                        await message.reply(
                                            f"⚠️ Produto `{asin}` não estava bloqueado.",
                                            delete_after=10
                                        )
                                        
                                except Exception as e:
                                    log_error(f"Erro ao desbloquear produto: {e}")
                                    await message.reply(
                                        f"❌ Erro ao desbloquear produto. Verifique os logs.",
                                        delete_after=10
                                    )
                            else:
                                await message.reply(
                                    f"❌ ASIN inválido. Use: `/desbloquear XXXXXXXXXX` (10 caracteres)",
                                    delete_after=10
                                )
                
                    except Exception as e:
                        log_error(f"Erro ao processar mensagem: {e}")
                
                try:
                    # Iniciar o bot
                    await client.start(DISCORD_TOKEN)
                except Exception as e:
                    log_error(f"Erro ao iniciar bot de reações: {e}")
                    print(f"❌ Falha ao iniciar bot de reações: {e}")
            
            # Executar o bot em um loop próprio
            try:
                asyncio.run(main())
            except Exception as e:
                log_error(f"Erro no bot de reações: {e}")
                print(f"❌ Bot de reações falhou: {e}")
        
        # Executar o bot em uma thread separada
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        print("🚀 Bot de reações iniciado em thread separada")
        
        return bot_thread

if __name__ == "__main__":
    import signal
    
    # Variável global para controlar o loop
    keep_running = True
    
    def signal_handler(signum, frame):
        global keep_running
        print("\n🛑 Recebido sinal de interrupção. Encerrando após o ciclo atual...")
        keep_running = False
        # Não há mais driver global para fechar
        if 'db' in globals():
            try:
                db.close()
            except:
                pass
        exit(0)
    
    # Configurar manipulador de sinais
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Função para executar um ciclo completo de scraping
    def run_scraping_cycle():
        try:
            # Inicializar o banco de dados
            db = ThreadSafeDatabase()
            
            # Carregar ASINs existentes
            with db_lock:
                cursor = db.get_connection().cursor()
                cursor.execute('SELECT asin FROM products')
                asins_gravados = {row[0] for row in cursor.fetchall()}
            
            print(f"\n📊 Total de produtos existentes: {len(asins_gravados)}")
            
            # Inicializar o cache de preços médios
            avg_price_cache = {}
            
            # Configurar opções do navegador
            global options
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
            # Suprimir logs do WebGL e GPU
            options.add_argument('--log-level=3')  # Níveis de log mais críticos apenas
            options.add_argument('--silent')
            options.add_argument('--disable-logging')
            
            # Desativar todos os tipos de logs e avisos
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-notifications')
            
            # Desativar acesso a dispositivos que podem gerar logs
            options.add_argument('--disable-usb')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-default-apps')
            
            # Desativar WebGL para evitar logs desnecessários
            options.add_argument('--disable-3d-apis')
            options.add_argument('--disable-webgl')
            options.add_argument('--disable-software-rasterizer')
            
            # Redirecionamento de logs para /dev/null 
            options.add_argument("--enable-logging")
            options.add_argument("--log-file=/dev/null")
            options.add_argument("--v=1")
            
            # Suprimir mensagens de erro específicas
            os.environ['PYTHONWARNINGS'] = 'ignore'
            
            # Desativar logs do ChromeDriver
            options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
            
            # Processar categorias em paralelo (5 threads)
            print("\n🚀 Iniciando processamento paralelo de categorias...")
            process_categories_parallel(DEFAULT_CATEGORIES, max_threads=5)
            print(f"\n✅ Ciclo concluído. Total de produtos únicos: {len(asins_gravados)}")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Erro durante o ciclo de scraping: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    try:
        # Inicializar o banco de dados uma vez
        db = ThreadSafeDatabase()
        
        # Testa a conexão com o Discord uma vez
        discord_connected = test_discord_connection()
        
        # Verificar se a conexão com o Discord foi estabelecida
        if not discord_connected:
            log_error("Não foi possível conectar ao Discord. As notificações não serão enviadas.")
            print("\n⚠️ AVISO: Conexão com o Discord falhou. O programa continuará, mas não enviará notificações.")
        else:
            # Se Discord conectado, iniciar bot de reações
            print("\n🤖 Iniciando bot de reações para bloqueio de produtos...")
            reaction_bot_thread = start_discord_reaction_bot()
            time.sleep(3)  # Dar tempo para o bot inicializar
        
        # Loop principal - executa indefinidamente
        cycle_count = 1
        
        while keep_running:
            try:
                print(f"\n{'='*60}")
                print(f"🔄 INICIANDO CICLO {cycle_count} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
                print(f"{'='*60}")
                
                # Executar um ciclo completo de scraping
                success = run_scraping_cycle()
                
                if success:
                    print(f"\n✅ Ciclo {cycle_count} concluído com sucesso!")
                else:
                    print(f"\n⚠️ Ciclo {cycle_count} concluído com erros.")
                
                cycle_count += 1
                
                # Aguardar 10 minutos antes do próximo ciclo
                if keep_running:
                    print(f"\n⏰ Aguardando 10 minutos antes do próximo ciclo...")
                    print(f"🕐 Próximo ciclo será às: {(datetime.now() + timedelta(minutes=10)).strftime('%d/%m/%Y %H:%M:%S')}")
                    
                    # Aguardar 10 minutos (600 segundos) com verificação a cada 30 segundos
                    for i in range(20):  # 20 * 30 segundos = 600 segundos (10 minutos)
                        if not keep_running:
                            break
                        time.sleep(30)
                        
                        # Mostrar progresso a cada 2 minutos
                        if (i + 1) % 4 == 0:
                            remaining_minutes = 10 - ((i + 1) * 0.5)
                            print(f"⏳ Restam {remaining_minutes:.0f} minutos para o próximo ciclo...")
                
            except KeyboardInterrupt:
                print("\n🛑 Interrupção detectada durante o ciclo.")
                break
            except Exception as e:
                print(f"\n❌ Erro no ciclo {cycle_count}: {e}")
                import traceback
                traceback.print_exc()
                
                # Aguardar um pouco antes de tentar novamente
                if keep_running:
                    print("\n⏰ Aguardando 5 minutos antes de tentar novamente...")
                    time.sleep(300)  # 5 minutos
        
        print(f"\n🏁 Programa encerrado após {cycle_count - 1} ciclos.")
        
    except KeyboardInterrupt:
        print("\n🛑 Execução interrompida pelo usuário.")
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Garantir que os recursos sejam liberados
        keep_running = False
        if 'db' in globals():
            try:
                db.close()
            except Exception as e:
                print(f"Aviso ao fechar o banco de dados: {e}")
