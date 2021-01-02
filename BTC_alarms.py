#Función envío alertas diarias de compra/venta por mail siguiendo la estrategia descubierta de las medias móviles de 9 + 23
#Estrategia descubierta detallada en https://colab.research.google.com/drive/1z4aJ592OgnPNAWRgr2BHt8SrU7xVkfT8

import pandas as pd
import time
from datetime import datetime, date, time, timedelta
from binance.client import Client
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def BTC_alarm(request):
	#GET DATA
	client = Client('x','x')

	# valid intervals - 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
	# get timestamp of earliest date data is available
	timestamp = client._get_earliest_valid_timestamp('BTCUSDT', '1d')

	# request historical candle (or klines) data
	bars = client.get_historical_klines('BTCUSDT', '1d', timestamp, limit=1000)

	# delete unwanted data - just keep date, open, high, low, close
	for line in bars:
	    del line[5:]

	# option 4 - create a Pandas DataFrame and export to CSV
	BTC_data = pd.DataFrame(bars, columns=['Date', 'BTC_24hOpenUSD', 'BTC_24hHighUSD', 'BTC_24hLowUSD', 'BTC_ClosingPriceUSD'])
	#-----------------------------------------------------------------------------------------------------


	#TRANSFORMACIÓN DATOS
	#Eliminamos las columnas que no utilizaremos en el estudio
	del BTC_data["BTC_24hHighUSD"]
	del BTC_data["BTC_24hLowUSD"]

	#Nos aseguramos que los datos deseados son numéricos
	BTC_data['BTC_ClosingPriceUSD'] = pd.to_numeric(BTC_data['BTC_ClosingPriceUSD'])
	BTC_data['BTC_24hOpenUSD'] = pd.to_numeric(BTC_data['BTC_24hOpenUSD'])

	#Nos aseguramos que el campo date es un datetime
	# BTC_data['Date'] = pd.to_datetime(BTC_data['Date'], format='%Y-%m-%d')
	BTC_data['Date'] = pd.to_datetime(BTC_data['Date'], unit='ms')

	#Nos aseguramos de los DataFrames están ordenados por fecha
	BTC_data = BTC_data.sort_values(by='Date', ascending=True)

	#Comprobación de que exite un único registro por fecha
	#Si hay algún registro sin fecha, lo crea e introduce los valores del día anterior, desde binance no he detectado ninguno
	num_rows = BTC_data.shape[0]-1
	i_i = BTC_data.index.min() #índice inicial
	d1 = BTC_data.loc[i_i, 'Date']
	d = 0 #contador de días
	i = i_i #contador de índices incluyendo nuevos registros
	n = i_i #contador de índices con solo los registros de las fechas encontradas

	while i <=num_rows:
	    comp_date = d1 + timedelta(days=d) #Fecha que tendremos que comprobar que existe
	    #Si existe la fecha
	    if BTC_data[BTC_data['Date']==comp_date]['Date'].size>0:
	        n+=1
	    #Si no existe la fecha
	    else:       
	        #Hallamos los datos del día anterior
	        closing_value = BTC_data.loc[n, 'BTC_ClosingPriceUSD']
	        open_value = BTC_data.loc[n, 'BTC_24hOpenUSD']
	        #Creamos el nuevo registro y le introducimos los datos del día anterior
	        # print("No existía registro de " + comp_date)
	        BTC_data.loc[num_rows+i]=[comp_date, closing_value, open_value]
	        BTC_data = BTC_data.sort_values(by='Date', ascending=True)
	        num_rows = BTC_data.shape[0]-1
	    i+=1
	    d+=1

	#Se reemplaza los valores por los BTC_ClosingPriceUSD del día anterior para asegurarnos que son correctos (descubrí que no lo eran, posiblemente por los llamados gaps en el precio de Bitcoin)
	#Despreciaremos el primer registro ya que no podremos encontrar el BTC_ClosingPriceUSD del día anterior
	BTC_data.loc[2:,"BTC_24hOpenUSD"] = BTC_data["BTC_ClosingPriceUSD"].shift(1)

	#Hallamos las fechas máximas y mínimas
	first_date = BTC_data['Date'].min()
	last_date = BTC_data['Date'].max()
	#-----------------------------------------------------------------------------------------------------


	#CALCULAR ALARMAS
	#Mejores medias móviles obtenidas en https://colab.research.google.com/drive/1z4aJ592OgnPNAWRgr2BHt8SrU7xVkfT8
	MA_a_max = 9
	MA_b_max = 23

	#Creamos las mejores medias móviles de cierre para los precios de apertura y cierre
	BTC_data["BTC_Closing_BMA_a"] = BTC_data["BTC_ClosingPriceUSD"].rolling(window=MA_a_max,min_periods=0).mean()
	BTC_data["BTC_Closing_BMA_b"] = BTC_data["BTC_ClosingPriceUSD"].rolling(window=MA_b_max,min_periods=0).mean()
	BTC_data["BTC_24Open_BMA_a"] = BTC_data["BTC_24hOpenUSD"].rolling(window=MA_a_max,min_periods=0).mean()
	BTC_data["BTC_24Open_BMA_b"] = BTC_data["BTC_24hOpenUSD"].rolling(window=MA_b_max,min_periods=0).mean()

	#Creamos las señales de alarma de compra y venta
	#Si en un día la media móvil de 10 estaba por debajo de la de 20 y termina el día por encima, señal de compra = 1
	BTC_data["BTC_buy_alarm"] = [1 if Open_BMA_b > Open_BMA_a and Closing_BMA_a > Closing_BMA_b else 0 for (Open_BMA_a,Open_BMA_b,Closing_BMA_a,Closing_BMA_b) in zip(BTC_data['BTC_24Open_BMA_a'],BTC_data['BTC_24Open_BMA_b'],BTC_data['BTC_Closing_BMA_a'],BTC_data['BTC_Closing_BMA_b'])] 

	#Si en un día la media móvil de 10 estaba por arriba de la de 20 y termina el día por debajo, señal de venta = 1
	BTC_data["BTC_sell_alarm"] = [1 if Open_BMA_b < Open_BMA_a and Closing_BMA_a < Closing_BMA_b else 0 for (Open_BMA_a,Open_BMA_b,Closing_BMA_a,Closing_BMA_b) in zip(BTC_data['BTC_24Open_BMA_a'],BTC_data['BTC_24Open_BMA_b'],BTC_data['BTC_Closing_BMA_a'],BTC_data['BTC_Closing_BMA_b'])]  

	buy = BTC_data.loc[BTC_data.index[-2], "BTC_buy_alarm"]
	sell = BTC_data.loc[BTC_data.index[-2], "BTC_sell_alarm"]
	comparation_MA = BTC_data.loc[BTC_data.index[-2], "BTC_Closing_BMA_a"] - BTC_data.loc[BTC_data.index[-2], "BTC_Closing_BMA_b"]
	#-----------------------------------------------------------------------------------------------------


	#CREACIÓN MENSAJE
	last_close_price = BTC_data.loc[BTC_data.index[-2], "BTC_ClosingPriceUSD"]
	last_open_price = BTC_data.loc[BTC_data.index[-2], "BTC_24hOpenUSD"]
	last_variation = (last_close_price-last_open_price)/last_open_price*100
	last_close_price_format = "{:,}".format(last_close_price).replace(',','~').replace('.',',').replace('~','.')
	last_open_price_format = "{:,}".format(last_open_price).replace(',','~').replace('.',',').replace('~','.')

	if last_variation>=0:
	    last_variation_format = '<span style="color: MediumSeaGreen">+'+ str(round(last_variation,2)) + '%</span>'
	else:
	    last_variation_format = '<span style="color: red">' + str(round(last_variation,2)) + '%</span>'

	extra_text = "<ul>  <li>Open: " + str(last_open_price_format) + "$</li>  <li>Close: " + str(last_close_price_format) + "$</li>  <li>Change: " + last_variation_format +"</li></ul>"
	
	subject_buy = "BTC BUY ALARM"
	message_buy = '<h1 style="color:MediumSeaGreen;">HA SALTADO LA SEÑAL DE COMPRA DE BTC</h1>'
	subject_sell = "BTC SELL ALARM"
	message_sell = '<h1 style="color:red;">HA SALTADO LA SEÑAL DE VENTA DE BTC</h1>'
	subject_stay_in = "BTC alarm updated"
	message_stay_in = "<h2>PERMANECE DENTRO DEL MERCADO</h2>"
	subject_stay_out = "BTC alarm updated"
	message_stay_out = "<h2>PERMANECE FUERA DEL MERCADO</h2>"

	if buy == 1:
	  subject = subject_buy
	  text = message_buy
	elif sell == 1:
	  subject = subject_sell
	  text = message_sell
	elif comparation_MA > 0:
	  subject = subject_stay_in
	  text = message_stay_in
	else:
	  subject = subject_stay_out
	  text = message_stay_out

	text_inicial = "<p>Se han actualizado las señales de compra/venta.</p>"
	text = text_inicial + text + extra_text
	
	sender_mail = 'javiermartiisasi@gmail.com'
	receiver_mail = 'javiermartiisasi@gmail.com'

	message = Mail(
	    from_email=sender_mail,
	    to_emails=receiver_mail,
	    subject=subject,
	    html_content=text)
	try:
	    sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
	    response = sg.send(message)
	    print(response.status_code)
	    print(response.body)
	    print(response.headers)
	    return "Ha funcionado"
	except Exception as e:
	    print(e.message)
	    return "No ha funcionado"
