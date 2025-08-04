#!/bin/bash

echo "=== Data Pipeline Technical Assessment Demo ==="
echo ""

echo "1. Verificando arquivos de entrada..."
ls -la tables/
echo ""

echo "2. Executando pipeline de dados..."
python3 data_pipeline_pandas.py
echo ""

echo "3. Verificando arquivos de saída..."
ls -la output/
echo ""

echo "4. Mostrando resultados da análise..."
echo "Top revenue color by year:"
python3 -c "
import pandas as pd
df = pd.read_parquet('output/analysis_top_color_by_year.parquet')
print(df.to_string(index=False))
"
echo ""

echo "Average lead time by product category:"
python3 -c "
import pandas as pd
df = pd.read_parquet('output/analysis_avg_lead_time_by_category.parquet')
print(df.to_string(index=False))
"
echo ""

echo "=== Demo concluído com sucesso! ===" 