# Challenge Data Science 11/2022

Um projeto Data Science com PySpark


## Insight Places

![Insight Places](resources/home_insigthplaces.png)


- Descrição
- Base de dados
- Dicionario de dados
- Estrutura da base
- Cronograma do projeto

## Descrição

A imobiliária **InsightPlaces**, situada na cidade do Rio de Janeiro, está enfrentando dificuldades para alugar e vender imóveis. Em uma pesquisa de como empresas semelhantes operam no mercado, a InsightPlaces percebeu que esse problema pode estar relacionado aos valores dos imóveis e às recomendações que faz.

Sou do time de Ciência de Dados e **Big Data** da InsightPlaces e fiquei responsável por auxiliar no processo de análise de dados dos imóveis localizados em alguns bairros da cidade do Rio de Janeiro.

Dentro desse contexto, como podemos definir de forma eficiente os preços dos imóveis lidando com grande volume de dados? É importante recomendar imóveis utilizando outro critério? O que precisa ser feito?

Esse projeto tem algumas etapas como: ler e fazer o tratamento do histórico dos preços de imóveis no Rio de Janeiro, construir um modelo de regressão para precificar imóveis e criar um recomendador de imóveis. Para cada uma dessas etapas vamos utilizar a ferramenta **PySpark** que oferece uma melhor performance ao trabalharmos com grandes volumes de dado

Vamos que vamos!

## Banco de dados 

[Base de dados - InsightPlaces](https://caelum-online-public.s3.amazonaws.com/challenge-spark/semana-1.zip)

### Dicionário de Dados

| Colunas         | Descrição                                                      |
|-----------------|----------------------------------------------------------------|
| id              | Código de identificação do anúncio no sistema da InsightPlaces |
| tipo_unidade    | Tipo de imóvel (apartamento, casa e outros)                    |
| tipo_uso        | Tipo de uso do imóvel (residencial ou comercial)               |
| area_total      | Área total do imóvel (construção e terreno)                    |
| area_util       | Área construída do imóvel                                      |
| quartos         | Quantidade de quartos do imóvel                                |
| suites          | Quantidade de suítes do imóvel                                 |
| banheiros       | Quantidade de banheiros do imóvel                              |
| vaga            | Quantidade de vagas de garagem do imóvel                       |
| caracteristicas | Listagem de características do imóvel                          |
| andar           | Número do andar do imóvel                                      |
| endereco        | Informações sobre o endereço do imóvel                         |
| valores         | Informações sobre valores de venda e locação dos imóveis       |


## Estrutura da base

Amostra de um registro
```JSON
{
  "anuncio":{
    "id":"47d553e0-79f2-4a46-9390-5a3c962740c2",
    "caracteristicas":[
      
    ],
    "area_util":[
      "16"
    ],
    "tipo_anuncio":"Usado",
    "tipo_unidade":"Outros",
    "andar":0,
    "vaga":[
      1
    ],
    "suites":[
      0
    ],
    "banheiros":[
      0
    ],
    "tipo_uso":"Comercial",
    "area_total":[
      
    ],
    "quartos":[
      0
    ],
    "valores":[
      {
        "iptu":"107",
        "condominio":"260",
        "valor":"10000",
        "tipo":"Venda"
      }
    ],
    "endereco":{
      "pais":"BR",
      "cep":"20061003",
      "cidade":"Rio de Janeiro",
      "longitude":-43.18671,
      "latitude":-22.906082,
      "zona":"Zona Central",
      "rua":"Rua Buenos Aires",
      "estado":"Rio de Janeiro",
      "bairro":"Centro"
    }
  },
  "usuario":{
    "id":"9d44563d-3405-4e84-9381-35b7cf40a9a4",
    "nome":"Frank"
  },
  "imagens":[
    {
      "id":"39d6282a-71f3-47bc-94aa-909351ecd881",
      "url":"https:\/\/api.images.insightplaces.com.br\/{type}\/{width}\/{height}\/1b08098d-f2dc-439c-b604-a4e50d12774b.jpg"
    }
  ]
}

```
## Etapas do projeto
### **SEMANA 01:**

Transformando dados com PySpark

Vamos explorar e tratar uma base de dados que veio dos sistemas internos da nossa empresa.


### **SEMANA 02:**

Tratando dados e criando um modelo de regressão com PySpark

Aqui você vai criar um modelo de regressão para prever os valores dos imóveis.

### **SEMANA 03 e 04:**
Criando um modelo de recomendação com PySpark
No fim do Challenge Data Science você criará um recomendador de imóveis.
