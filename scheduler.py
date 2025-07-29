from config import cfg
from models import JobHE, JobDE, Weekday, Log
from sqlalchemy import text
from auxils import is_select_query
from datetime import datetime, timedelta

from concurrent.futures import ThreadPoolExecutor

import oracledb
import schedule
import csv
import time
import os

# --- Import Logging ---
from logging_config import get_logger, log_info, log_warning, log_error, log_exception, log_debug
logger = get_logger('scheduler')
# --- End Logging Import ---

## CONSTANTES

# abreviações PT → método schedule em inglês
DAY_MAP = {
    'Seg': 'monday',
    'Ter': 'tuesday',
    'Qua': 'wednesday',
    'Qui': 'thursday',
    'Sex': 'friday',
    'Sáb': 'saturday',
    'Dom': 'sunday'
}

# Thread pool (ajuste max_workers conforme CPUs / volume de jobs)
# TODO: Implementar numero de workers no datafile.json
executor = ThreadPoolExecutor(max_workers=6)


def generate_time_slots(hora_ini, hora_fim, periodicity):
    fmt   = "%H:%M"
    inicio = datetime.strptime(hora_ini, fmt)
    slots = []
    
    if hora_fim is None:
        slots.append(inicio.strftime(fmt))
    else:        
        fim    = datetime.strptime(hora_fim, fmt)
        delta = timedelta(minutes=float(periodicity))

        atual    = inicio
        # Vai adicionando até chegar (ou ultrapassar) o fim
        while atual <= fim:
            slots.append(atual.strftime(fmt))
            atual += delta
        
        del fim, delta, atual
    
    del inicio, hora_ini, hora_fim, periodicity

    return slots


def fetch_jobs(job_id=None):
    PostgreSession = cfg.get_postgres_session()

    log_debug(logger, f"Fetching jobs from DB. Specific job_id: {job_id if job_id else 'All active'}")

    try:
        # só traz aqueles com status = 'Y' (ativo)
        if job_id is None:
            with PostgreSession() as session:
                jobs = session.query(JobHE).filter_by(job_status='Y').all()
        else:
            with PostgreSession() as session:
                jobs = session.query(JobHE).filter_by(job_status='Y', job_id=job_id).all()

        result = []

        job_ids_fetched = [j.job_id for j in jobs]
        if not job_ids_fetched:
                log_debug(logger, "No active jobs found matching criteria.")
                return result # Vazio

        for job in jobs:
            # coleta os horários/dias associados

            with PostgreSession() as session:
                scheds = session.query(JobDE).filter_by(job_id=job.job_id).all()
            
            # Cada linha de scheds é um dia da semana
            for s in scheds:

                # cria slots de hora
                """
                Obtem `start_hour`, `end_hour` e `job_iter`
                e cria uma lista com todos os horários possíveis com as combinações.
                """
                time_slots = generate_time_slots(s.start_hour, s.end_hour, s.job_iter)

                for ts in time_slots:
                    result.append({
                        'job_id': job.job_id,
                        'schedule_id': s.schedule_id,
                        'name': job.job_name,
                        'export_path': job.export_path,
                        'export_name': job.export_name,
                        'sql_script': job.sql_script,
                        'day': s.job_day,
                        'time': ts
                    })
        
        log_debug(logger, f"Fetched {len(result)} job schedule instances.")
        return result
    except Exception as e:
        log_exception(logger, f"Error fetching jobs from database: {e}")
        return [] # Return empty list on error


def execute_job(job_data):
    start_time = time.time()
    PostgreSession = cfg.get_postgres_session()
    job_logger = get_logger('executor')

    # job infos
    job_id = job_data.get('job_id', None)
    job_name = job_data.get('name', 'Unknown Job')
    absolute_path = None

    log_info(job_logger, f"Starting job execution: '{job_name}'", job_id=job_id)
    
    def set_exec_time():
        with PostgreSession() as session:
            job = session.get(JobHE, job_id)

            if job:
                job.last_exec = datetime.now()
                session.commit()

    try:
        archive_path = job_data['export_path']
        archive_name_with_extention = job_data['export_name'] + '.csv'

        sql = job_data['sql_script']

        if not sql:
            log_error(job_logger, f"Job '{job_name}' has no SQL script defined.", job_id=job_id)
            return
    
        absolute_path = os.path.join(archive_path, archive_name_with_extention)
        log_debug(job_logger, f"Job '{job_name}': Export path: {absolute_path}", job_id=job_id)

        # Ensure target directory exists
        os.makedirs(archive_path, exist_ok=True)

        # Verifica se o comando é DQL
        if not is_select_query(sql):
            log_error(job_logger, f"Job '{job_name}': SQL is not a SELECT query. Aborting.", job_id=job_id)
            return

        log_debug(job_logger, f"Job '{job_name}': Executing SQL:\n{sql[:200]}...", job_id=job_id)
        rows_exported = 0

        OracleSession = cfg.get_oracle_session()

        # Execução do SQL e exportação com fetchmany()
        with OracleSession() as session:
            # 2) Executa o seu SQL com stream_results para permitir fetchmany
            stmt = text(sql).execution_options(stream_results=True)
            result = session.execute(stmt)

            # 4) Abre CSV e escreve cabeçalho
            with open(absolute_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter=';')
                writer.writerow(result.keys())

                # 5) Itera em blocos de até `arraysize`
                while True:
                    batch = result.fetchmany()
                    if not batch:
                        break
                    writer.writerows(batch)
                    rows_exported += len(batch)
                    log_debug(
                        job_logger,
                        f"Job '{job_name}': Fetched/wrote {len(batch)} rows (Total: {rows_exported})",
                        job_id=job_id
                    )
        
        end_time = time.time()
        duration_ms = int((end_time - start_time) * 1000)

        set_exec_time()

        log_info(job_logger, f"Job '{job_name}' finished successfully. Exported {rows_exported} rows.", job_id=job_id, duration_ms=duration_ms)

    except FileNotFoundError:
        set_exec_time()
        log_exception(job_logger, f"Job '{job_name}': Error creating/writing file at '{absolute_path}'. Check path and permissions.", job_id=job_id)
    except oracledb.DatabaseError as ora_err:
         set_exec_time()
         log_exception(job_logger, f"Job '{job_name}': Oracle Database Error during execution: {ora_err}", job_id=job_id)
    except Exception as error:
        set_exec_time()
        end_time = time.time()
        duration_ms = int((end_time - start_time) * 1000)
        # Use log_exception to include traceback
        log_exception(job_logger, f"Job '{job_name}': Unexpected error during execution: {error}", job_id=job_id, duration_ms=duration_ms)
        # Optionally re-raise if needed elsewhere, but likely not in a scheduled task
        # return # Ensure function exits on error

def schedule_job(jobs=None):
    """
    - Se job for None: carrega TODOS os registros do banco e agenda cada um.
    - Se job for um dict: agenda apenas esse job em memória.
    - Se job for um int (job_id): busca esse registro no banco e agenda.
    """
    log_source = "database (all active)"
    if jobs is None:
        jobs = fetch_jobs()
        log_info(logger, f"Scheduling all active jobs from database.")

        # a cada 2 horas, faz o reload completo:
        schedule.every(2).hours.do(lambda: (
            schedule.clear(),   # limpa tudo
            schedule_job()      # recarrega todos
        ))
        log_info(logger, "Scheduled periodic job reload every 2 hours.")
    elif type(jobs) == int:
        log_source = f"database (ID: {jobs})"
        jobs = fetch_jobs(job_id=jobs)
        log_info(logger, f"Scheduling specific job from database: ID {jobs}.")
    elif type(jobs) == dict:
        log_source = "provided list"
        log_info(logger, f"Scheduling {len(jobs)} jobs from provided list.")
        # Assume list of dicts is correctly formatted
    else:
        log_error(logger, f"Invalid input type for schedule_job: {type(jobs)}. Expected None, int, or list[dict].")
        return
    
    scheduled_count = 0

    # TODO: Implementar outra alternativa para este LOOP, a variavel job pode possivelmente não existir.
    # O loop foi refatorado para que o tratamento de exceção ocorra por item,
    # garantindo que a variável 'job' sempre exista no bloco 'except' e permitindo
    # que o processo continue mesmo se um job individual falhar.

    log_info(logger, "Starting job scheduling...")
    scheduled_count = 0

    # Adicionamos um 'try' externo para capturar erros com a própria lista de jobs (ex: se for None ou não iterável)
    try:
        # Verificamos se a lista de jobs existe e não está vazia antes de iterar
        if not jobs:
            log_warning(logger, "Job list is empty or None. No jobs to schedule.")
        else:
            for job in jobs:
                try:
                    # A lógica de processamento de um único job fica dentro deste 'try'
                    day_key = job['day']
                    tag = job['job_id']
                    hhmm = job['time']

                    day_method = DAY_MAP.get(day_key)
                    if not day_method:
                        log_warning(logger, f"Invalid day '{day_key}' for job '{job['name']}' (ID: {tag}). Skipping this schedule.", job_id=tag)
                        continue  # Pula para o próximo job

                    # Usar uma função anônima (lambda) é mais conciso aqui
                    job_wrapper = lambda job_data=job: (
                        log_debug(logger, f"Submitting job '{job_data['name']}' (ID: {job_data['job_id']}) to executor.", job_id=job_data['job_id']),
                        executor.submit(execute_job, job_data)
                    )

                    # Comentado: Gera muitos logs
                    # log_info(logger, f"Scheduling job '{job['name']}' (Tag: {tag}) for {day_key} at {hhmm}", job_id=tag)
                    getattr(schedule.every(), day_method).at(hhmm).do(job_wrapper).tag(tag)
                    
                    # Incrementa o contador apenas se o agendamento for bem-sucedido
                    scheduled_count += 1

                except KeyError as e:
                    # Este 'except' agora captura erros de um job específico.
                    # A variável 'job' está garantida de existir aqui.
                    job_id = job.get('job_id', 'N/A')
                    log_error(logger, f"Missing key {e} in data for job '{job.get('name', 'Unknown')}'. Skipping this schedule.", job_id=job_id)
                except Exception as e:
                    # O mesmo vale para outras exceções inesperadas.
                    job_id = job.get('job_id', 'N/A')
                    log_exception(logger, f"Unexpected error scheduling job '{job.get('name', 'Unknown')}': {e}", job_id=job_id)

    except TypeError:
        # Este 'except' captura o erro se 'jobs' não for uma lista/iterável.
        log_error(logger, "Job configuration is invalid. 'jobs' is not an iterable collection.")
    except Exception as e:
        # Captura qualquer outro erro inesperado no nível superior.
        log_exception(logger, f"A critical error occurred during the scheduling process: {e}")


    log_info(logger, f"Finished scheduling. Added {scheduled_count} schedule entries.")


def run_loop():
    log_info(logger, "Scheduler run_loop starting.")
    log_info(logger, f"Next scheduled run at: {schedule.next_run}")
    while True:
        try:
            schedule.run_pending()

            idle = schedule.idle_seconds()
            if idle is None:
                # No jobs scheduled
                log_debug(logger, "No jobs scheduled. Sleeping for 120 seconds.")
                time.sleep(120)
            elif idle > 0:
                # Sleep until the next job, but check more frequently than idle_seconds
                # Check every 60 seconds or until next job, whichever is smaller
                sleep_time = min(idle, 60)
                log_debug(logger, f"Next job in {idle:.2f} seconds. Sleeping for {sleep_time:.2f} seconds.")
                time.sleep(sleep_time)
            else:
                # Jobs might be due now or overdue, sleep very briefly
                 log_debug(logger, "Jobs pending or due. Short sleep (1s).")
                 time.sleep(10) # Short sleep if jobs ran or are due

        except KeyboardInterrupt:
             log_info(logger, "Scheduler run_loop interrupted by user (KeyboardInterrupt). Exiting.")
             break
        except Exception as e:
             log_exception(logger, f"Error in scheduler run_loop: {e}. Continuing loop.")
             time.sleep(30) # Sleep a bit longer after an error in the loop itself


if __name__ == '__main__':
    log_info(logger, "*** Scheduler Service Starting ***")
    try:
        schedule_job() # Initial scheduling
        run_loop()
    except Exception as e:
        log_exception(logger, "*** Scheduler Service Crashed Unhandled Exception ***")
    finally:
        log_info(logger, "*** Scheduler Service Shutting Down ***")
        executor.shutdown(wait=True) # Wait for running jobs to finish if possible