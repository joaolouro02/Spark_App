# Spark_App

Para a resolução da 1ª parte do desafio começou-se por utilizar o jupyter notebook para pré-processar os dados do dataset googleplaystore_user_reviews.csv, visto que havia inúmeras apps com valores nulos nas colunas Translated_Review, Sentiment, Sentiment_Polarity e Sentiment_Subjectivity. Uma vez que era para calcular a média do Sentiment_Polarity para cada app não faria sentido colocar 0's em avaliações que não estão completas o que iria prejudicar a avaliação das apps, por esse motivo essas linhas foram removidas e não foram tidas em consideração.
