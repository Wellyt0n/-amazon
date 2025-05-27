# 🛒 Amazon Price Monitor Bot

Bot automatizado para monitoramento de preços da Amazon com notificações no Discord.

## 📋 Funcionalidades

- ✅ **Scraping automatizado** de múltiplas categorias da Amazon
- ✅ **Detecção de descontos** em tempo real (20%+ de desconto)
- ✅ **Notificações no Discord** com embeds personalizados
- ✅ **Sistema de bloqueio** de produtos via reações
- ✅ **Histórico de preços** com banco SQLite
- ✅ **Execução contínua** com ciclos de 10 minutos
- ✅ **Múltiplas categorias**: Eletrônicos, Alimentos, Beleza, Bebê, etc.

## 🚀 Como usar

### 1. Configuração inicial

```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar variáveis de ambiente (.env)
DISCORD_TOKEN=seu_token_aqui
CHANNEL_20_40=id_canal_desconto_20_40
CHANNEL_40_70=id_canal_desconto_40_70
CHANNEL_70_100=id_canal_desconto_70_100
DISCORD_ALIMENTOS_ID=id_canal_alimentos
DISCORD_BEBE_ID=id_canal_bebe
DISCORD_BELEZA_ID=id_canal_beleza
```

### 2. Executar o bot

```bash
python backup.py
```

## 📊 Estrutura do Banco de Dados

- **products**: Produtos monitorados
- **price_history**: Histórico de preços
- **blocked_products**: Produtos bloqueados
- **sent_notifications**: Notificações enviadas

## 🔧 Configurações

### Categorias Monitoradas

- 🖥️ Eletrônicos e Informática
- 🍽️ Alimentos e Bebidas
- 👶 Produtos para Bebê
- 💄 Beleza e Cuidados Pessoais
- 🏠 Casa e Jardim
- 🔧 Ferramentas e Construção
- 🎮 Games e Brinquedos
- 🚗 Automotivo

### Sistema de Canais Discord

- **20-40% desconto**: Produtos com desconto moderado
- **40-70% desconto**: Produtos com desconto alto
- **70-100% desconto**: Produtos com desconto extremo
- **Canais específicos**: Alimentos, Bebê, Beleza

## 🤖 Bot de Reações

- Reaja com ❌ em qualquer notificação para bloquear o produto
- Use `/desbloquear ASIN` para desbloquear produtos

## 📝 Logs

- **erros.log**: Registro de erros do sistema
- **notificacoes.log**: Histórico de notificações enviadas

## ⚙️ Funcionalidades Avançadas

- **Detecção de descontos acumulados**: Monitora quedas de preço ao longo do tempo
- **Prevenção de spam**: Evita notificações duplicadas
- **Recuperação de erros**: Continua funcionando mesmo com falhas temporárias
- **Otimização de banco**: Limpeza automática de dados antigos

## 🛡️ Segurança

- Produtos com "Cabo" no título são automaticamente bloqueados
- Sistema de rate limiting para evitar bloqueios
- Verificação de produtos bloqueados antes de notificar

## 📈 Estatísticas

O bot monitora automaticamente:
- Total de produtos únicos
- Notificações enviadas por ciclo
- Produtos bloqueados
- Performance do sistema

---

**Desenvolvido com ❤️ para encontrar as melhores ofertas da Amazon!** 