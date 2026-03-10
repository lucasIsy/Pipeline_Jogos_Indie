from airflow.models import DagBag

def test_dag_integrity():
    # O DagBag procura arquivos .py na pasta especificada
    dag_bag = DagBag(dag_folder='airflow/dags/', include_examples=False)
    
    # O teste falha se houver qualquer erro de importação
    assert len(dag_bag.import_errors) == 0, f"Erros encontrados: {dag_bag.import_errors}"