import telebot

# TOKEN TELEGRAM bot ivy
TOKEN = '7359250833:AAFTwh35Tp9-1qIIiGY9AMpq2m2tcai01-k'

# Conexion BD Maximo
user_maximo = 'CGDASHBOARD'
psw_maximo = 'CgMovistar19'

# Conexion PTM
user_ptm = 'cgestion'
psw_ptm = 'T3l3f0n1c4'

# Conexion ROSE
user_rose = 'telefonica_user2'
psw_rose = 't3l3fon1ca#2024'

# Conexión API Maximo Desarrollo
userdev = 'restusr'
passdev = 'restusr2023'
# Conexión API Maximo Producción

userpro='CENTROGESTION'
passpro='Centrogestion2025'

# Define el token y crea el bot
TOKEN = '7359250833:AAFTwh35Tp9-1qIIiGY9AMpq2m2tcai01-k'
bot = telebot.TeleBot(TOKEN)

# Manejador del comando /start
@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, "¡Hola! Bot conectado con éxito ✅")

def main():
    print("🚀 Bot encendido")
    bot.infinity_polling()

if __name__ == '__main__':
    main()
    