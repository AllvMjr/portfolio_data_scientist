import os
from datetime import datetime, timedelta

class strutils:

    def get_data_dd_mm_yyyy_by_isoformat(input_data):
        """
        Recebe data no formato: 2023-07-10T15:13:11.767-0300
        Retorna data no formato: 10/07/2023
        """
        try:
            year = input_data[:4]
            month = input_data[5:-21]
            day = input_data[8:-18]

            formatted_date = input_data[:10]
            formatted_date = day + "/" + month + "/" + year
            return formatted_date
        except ValueError:
            print("Erro: Formato de data inválido.")
            return None
        
    def get_data_search_access(input_data):
        if input_data is not None:
            year = input_data[slice(0, 4)]
            mes = input_data[slice(5, 7)]
            dia = input_data[slice(8, 10)]
            
            data = f"{mes}-{dia}-{year}"
            return data
    
    def diferenca_entre_datas(data_inicio: str, data_final: str) -> int:        
        try:
            inicio = datetime.strptime(data_inicio, '%Y-%m-%d')
            final = datetime.strptime(data_final, '%Y-%m-%d')
            diferenca = final - inicio
            return diferenca.days
        except ValueError:
            print(f"inicio: {data_inicio}  final: {data_final}")
            print("Erro: Formato de data inválido.")
            return 0 
        
        
    def cut_data(input_data):
        try:
            retiradata = input_data[slice(0,10)]
            return retiradata
        except ValueError:
            return 0
        
    def get_sat_sun(input_data):
        checa_segunda = input_data.weekday()
        
        if checa_segunda == 0:
            sexta_passada = input_data - timedelta(days=3)
            return sexta_passada
        else:
            data_anterior = input_data - timedelta(days=1)
            return data_anterior
    
    def get_interation_weekly(input_data):
        data_inicio_iteracao = ''
        data_fim_iteracao = ''
        semana = input_data.isocalendar().week
        numdia = input_data.weekday()
        
        if numdia == 0: # Segunda
            data_inicio_iteracao = input_data
            data_fim_iteracao = input_data + timedelta(days=4)
        elif numdia == 1:# Terça
            data_inicio_iteracao = input_data - timedelta(days=1)
            data_fim_iteracao = input_data + timedelta(days=3)
        elif numdia == 2: # Quarta
            data_inicio_iteracao = input_data - timedelta(days=2)
            data_fim_iteracao = input_data + timedelta(days=2)
        elif numdia == 3: # Quinta
            data_inicio_iteracao = input_data - timedelta(days=3)
            data_fim_iteracao = input_data + timedelta(days=1)
        elif numdia == 4: # Sexta
            data_inicio_iteracao = input_data - timedelta(days=4)
            data_fim_iteracao = input_data
        elif numdia == 5: # Sabado
            data_inicio_iteracao = input_data - timedelta(days=5)
            data_fim_iteracao = input_data - timedelta(days=1)
        elif numdia == 6:
            data_inicio_iteracao = input_data - timedelta(days=6)
            data_fim_iteracao = input_data - timedelta(days=2)

        return {'semana': semana, 
                'inicio_iteracao': f'{data_inicio_iteracao.strftime("%Y-%m-%d")}', 
                'fim_iteracao': f'{data_fim_iteracao.strftime("%Y-%m-%d")}',
                'cut_inicio': f'{data_inicio_iteracao.strftime("%d-%m")}',
                'cut_fim': f'{data_fim_iteracao.strftime("%d-%m")}'}
    
    def get_next_interation_weekly(input_data):
        data_inicio_iteracao = ''
        data_fim_iteracao = ''
        semana = input_data.isocalendar().week
        numdia = input_data.weekday()
        
        if (numdia == 5):
            proximadata = input_data + timedelta(days=2)
        elif (numdia == 6):
            proximadata = input_data + timedelta(days=1)
        else:
            proximadata = input_data

        semana = proximadata.isocalendar().week
        numdia = proximadata.weekday()
            
        if numdia == 0: # Segunda
            data_inicio_iteracao = proximadata
            data_fim_iteracao = proximadata + timedelta(days=4)
        elif numdia == 1:# Terça
            data_inicio_iteracao = proximadata - timedelta(days=1)
            data_fim_iteracao = proximadata + timedelta(days=3)
        elif numdia == 2: # Quarta
            data_inicio_iteracao = proximadata - timedelta(days=2)
            data_fim_iteracao = proximadata + timedelta(days=2)
        elif numdia == 3: # Quinta
            data_inicio_iteracao = proximadata - timedelta(days=3)
            data_fim_iteracao = proximadata + timedelta(days=1)
        elif numdia == 4: # Sexta
            data_inicio_iteracao = proximadata - timedelta(days=4)
            data_fim_iteracao = proximadata
        elif numdia == 5: # Sabado
            data_inicio_iteracao = proximadata - timedelta(days=5)
            data_fim_iteracao = proximadata - timedelta(days=1)
        elif numdia == 6:
            data_inicio_iteracao = proximadata - timedelta(days=6)
            data_fim_iteracao = proximadata - timedelta(days=2)

        return {'semana': semana, 
                'inicio_iteracao': f'{data_inicio_iteracao.strftime("%Y-%m-%d")}', 
                'fim_iteracao': f'{data_fim_iteracao.strftime("%Y-%m-%d")}',
                'cut_inicio': f'{data_inicio_iteracao.strftime("%d-%m")}',
                'cut_fim': f'{data_fim_iteracao.strftime("%d-%m")}'}
    
    def get_iteration_biweekly(input_data):        
        data_anterior = input_data - timedelta(days=11)
        #data_anterior = input_data
        return data_anterior
    
    def get_next_biinteration_weekly_handle(input_data1, input_data2):
        data_ini = datetime.strptime(input_data1, '%Y-%m-%d')
        data_fim = datetime.strptime(input_data2, '%Y-%m-%d')
        cut_ini = data_ini.strftime('%d-%m')
        cut_fim = data_fim.strftime('%d-%m')
        semana = data_ini.isocalendar().week

        return {'semana': semana, 
                'inicio_iteracao': f'{data_ini.strftime("%Y-%m-%d")}', 
                'fim_iteracao': f'{data_fim.strftime("%Y-%m-%d")}',
                'cut_inicio': f'{cut_ini}',
                'cut_fim': f'{cut_fim}'}

    def get_next_binteration_weekly(input_data):
        data_inicio_iteracao = ''
        data_fim_iteracao = ''
        semana = input_data.isocalendar().week
        numdia = input_data.weekday()
        
        if (numdia == 5):
            proximadata = input_data + timedelta(days=2)
        elif (numdia == 6):
            proximadata = input_data + timedelta(days=1)
        else:
            proximadata = input_data

        semana = proximadata.isocalendar().week
        numdia = proximadata.weekday()

        if numdia == 0: # Segunda
            data_inicio_iteracao = proximadata
            data_fim_iteracao = proximadata + timedelta(days=11)
        
        print(f"Data_inicio: {data_inicio_iteracao} - Data fim: {data_fim_iteracao}")

        """  
        if numdia == 0: # Segunda
            data_inicio_iteracao = proximadata
            data_fim_iteracao = proximadata + timedelta(days=4)
        elif numdia == 1:# Terça
            data_inicio_iteracao = proximadata - timedelta(days=1)
            data_fim_iteracao = proximadata + timedelta(days=3)
        elif numdia == 2: # Quarta
            data_inicio_iteracao = proximadata - timedelta(days=2)
            data_fim_iteracao = proximadata + timedelta(days=2)
        elif numdia == 3: # Quinta
            data_inicio_iteracao = proximadata - timedelta(days=3)
            data_fim_iteracao = proximadata + timedelta(days=1)
        elif numdia == 4: # Sexta
            data_inicio_iteracao = proximadata - timedelta(days=4)
            data_fim_iteracao = proximadata
        elif numdia == 5: # Sabado
            data_inicio_iteracao = proximadata - timedelta(days=5)
            data_fim_iteracao = proximadata - timedelta(days=1)
        elif numdia == 6:
            data_inicio_iteracao = proximadata - timedelta(days=6)
            data_fim_iteracao = proximadata - timedelta(days=2)

        return {'semana': semana, 
                'inicio_iteracao': f'{data_inicio_iteracao.strftime("%Y-%m-%d")}', 
                'fim_iteracao': f'{data_fim_iteracao.strftime("%Y-%m-%d")}',
                'cut_inicio': f'{data_inicio_iteracao.strftime("%d-%m")}',
                'cut_fim': f'{data_fim_iteracao.strftime("%d-%m")}'}
        """

        
            


    

            
        
        
            