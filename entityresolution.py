import csv
from neo4j import GraphDatabase
from gensim.models import KeyedVectors
import textdistance
import sys


def writecsv(file, string):
    with open(file + '.csv', 'a', encoding="utf-8") as outfile:
        outfile.write(string + "\n")


def createEdgeListProductManufacturerCertificates(driver, filename):
    print('Create the edge list')
    with driver.session() as session, open("graph/" + filename + ".edgelist", "a", encoding='UTF8') as edges_file:

        # create edge manufaktur
        result = session.run("""\
        MATCH (m:ns1__FoodProduct)-[:ns3__hasManufacturer]-(other)
        RETURN id(m) AS source, id(other) AS target
        """)

        writer = csv.writer(edges_file, delimiter=" ")

        for row in result:
            writer.writerow([row["source"], row["target"]])

        # create edge certificates
        result = session.run("""\
                MATCH (m:ns1__FoodProduct)-[:ns1__certificate]-(other)
                RETURN id(m) AS source, id(other) AS target
                """)

        writer = csv.writer(edges_file, delimiter=" ")

        for row in result:
            writer.writerow([row["source"], row["target"]])


def createEdgeListProductManufacturerCertificatesIngrediens(driver, filename):
    print('Create the edge list')
    with driver.session() as session, open("graph/" + filename + ".edgelist", "a", encoding='UTF8') as edges_file:

        # create edge manufaktur
        result = session.run("""\
        MATCH (m:ns1__FoodProduct)-[:ns3__hasManufacturer]-(other)
        RETURN id(m) AS source, id(other) AS target
        """)

        writer = csv.writer(edges_file, delimiter=" ")

        for row in result:
            writer.writerow([row["source"], row["target"]])

        # create edge certificates
        result = session.run("""\
                MATCH (m:ns1__FoodProduct)-[:ns1__certificate]-(other)
                RETURN id(m) AS source, id(other) AS target
                """)

        writer = csv.writer(edges_file, delimiter=" ")

        for row in result:
            writer.writerow([row["source"], row["target"]])

        # create edge containsingredient
        result = session.run("""\
                        MATCH (m:ns1__FoodProduct)-[:ns2__containsIngredient]-(other)
                        RETURN id(m) AS source, id(other) AS target
                        """)

        writer = csv.writer(edges_file, delimiter=" ")

        for row in result:
            writer.writerow([row["source"], row["target"]])


def writeEmbedtoNode(driver, filename):
    with open("emb/" + filename + ".emb", "r") as halal_file, driver.session() as session:
        next(halal_file)
        reader = csv.reader(halal_file, delimiter=" ")

        params = []
        for row in reader:
            entity_id = row[0]
            params.append({
                "id": int(entity_id),
                "embedding": [float(item) for item in row[1:]]
            })

        for param in params:
            print(str(param['id']) + " - " + str(param['embedding']))

        session.run("""\
        UNWIND {params} AS param
        MATCH (m:ns1__FoodProduct) WHERE id(m) = param.id
        SET m.embedding = param.embedding
        """, {"params": params})


def createOwlSameAsRelationQuery(driver, id_source, id_target):
    with driver.session() as session:
        find_entity_query = "MATCH (a:ns1__FoodProduct),(b:ns1__FoodProduct) WHERE id(a) = {} AND id(b) = {} CREATE (a)-[r:owl__sameAs]->(b) RETURN r".format(
            id_source, id_target)
        session.run(find_entity_query)


def createRdfsSeeAlsoRelationQuery(driver, id_source, id_target):
    with driver.session() as session:
        find_entity_query = "MATCH (a:ns1__FoodProduct),(b:ns1__FoodProduct) WHERE id(a) = {} AND id(b) = {} CREATE (a)-[r:rdfs__seeAlso]->(b) RETURN r".format(
            id_source, id_target)
        session.run(find_entity_query)


def createLinkCountProperti(driver, id_source, linkcount):
    with driver.session() as session:
        query = "MATCH (a:ns1__FoodProduct) WHERE id(a) = {} SET a.linkCount = {}".format(id_source, linkcount)
        session.run(query)


def getEntityProfilesCandidate(driver, model, entityId):
    entity_id = entityId
    tupple_source = getEntityDetailsNameAndManuFacture(driver, entity_id)
    similarIds = neo4j_most_similarById(driver, model, entity_id)
    listEntityProfilesCandidate = []
    for similarId in similarIds:
        tupple_target = getEntityDetailsNameAndManuFacture(driver, similarId[0])

        threshold = 0.7
        # print("Start comparing without ingredients id = " + str(entity_id) + " with " + str(similarId[0]))
        try:
            similarityProductName = 0
            try:
                # print("Coba = "+str(tupple_target))
                similarityProductName = checkSimilarityJaro(tupple_source[0][1], tupple_target[0][1])
                # print("comparing productname = " + str(tupple_source[0][1]) + " with " + tupple_target[0][
                #     1] + " similarity score = " + str(similarityProductName))
                stringPrint = str(similarityProductName) + " - " + tupple_target[0][1] + " - " + str(
                    tupple_source[0][1])
                # print("Result similarity = "+stringPrint)
            except Exception as ex:
                print("Error comparing product name ex =" + str(ex))
            similarity = similarityProductName
            if (similarity > threshold):
                listEntityProfilesCandidate.append(similarId[0])

            else:
                # print("Similarity score is not enough = " + str(similarity))
                print("")

        except Exception as ex:
            print("error when trying to comparing entities tanpa ingredients = " + str(ex))

    listEntityProfilesCandidate.sort()

    return listEntityProfilesCandidate


def performEntityResolution(driver, entitySourceId, entityTargetId):
    tupple_source_with_ingredients = getEntityDetailsNameAndManuFactureAndIngredients(driver, entitySourceId)
    tupple_target_with_ingredients = getEntityDetailsNameAndManuFactureAndIngredients(driver, entityTargetId)
    ingredientsSource = ""
    ingredientsTarget = ""
    if (len(tupple_source_with_ingredients) != 0):
        print("Product source has ingredients")
        try:
            for tupple_source_with_ingredient in tupple_source_with_ingredients:
                ingredientsSource = ingredientsSource + ", " + tupple_source_with_ingredient[3]
            # print(ingredientsSource)
        except Exception as ex:
            print("Error when concenating string ingredients = " + str(ex))

    if (len(tupple_target_with_ingredients) != 0):
        print("Product target has ingredients")
        try:
            for tupple_target_with_ingredient in tupple_target_with_ingredients:
                ingredientsTarget = ingredientsTarget + ", " + tupple_target_with_ingredient[3]
            # print(ingredientsTarget)
        except Exception as ex:
            print("Error when concenating string ingredients = " + str(ex))

    threshold = 0.9

    isThereOwlSameAs = False
    stringToWrite = ""

    if (len(tupple_source_with_ingredients) == 0 or len(tupple_target_with_ingredients) == 0):
        try:
            tupple_source = getEntityDetailsNameAndManuFacture(driver, entitySourceId)
            tupple_target = getEntityDetailsNameAndManuFacture(driver, entityTargetId)
            print("Comparing = " + tupple_source[0][1] + " with " + tupple_target[0][1])
            similarityScoreProduct = checkSimilarityJaro(tupple_source[0][1], tupple_target[0][1])
            print("Similarity product = " + str(similarityScoreProduct))
            print("Comparing = " + tupple_source[0][2] + " with " + tupple_target[0][2])
            similarityScoreManufacturer = checkSimilarityJaro(tupple_source[0][2], tupple_target[0][2])
            print("Similarity manufacturer = " + str(similarityScoreManufacturer))
            similarityTotal = (0.8 * similarityScoreProduct) + (0.2 * similarityScoreManufacturer)
            print("Similarity Total = " + str(similarityTotal))
            if similarityTotal >= threshold:
                print("Gotcha, there is owl:sameAs")
                createOwlSameAsRelationQuery(driver, entitySourceId, entityTargetId)
                isThereOwlSameAs = True

                stringToWrite = str(entitySourceId) + "|" + str(tupple_source[0][1]) + "|" + str(
                    tupple_source[0][2]) + "|" + ingredientsSource + "|" + \
                                str(entityTargetId) + "|" + str(tupple_target[0][1]) + "|" + str(
                    tupple_target[0][2]) + "|" + ingredientsTarget + "|owl:sameAs|" + str(similarityTotal)
            else:
                createRdfsSeeAlsoRelationQuery(driver, entitySourceId, entityTargetId)
                stringToWrite = str(entitySourceId) + "|" + str(tupple_source[0][1]) + "|" + str(
                    tupple_source[0][2]) + "|" + ingredientsSource + "|" + \
                                str(entityTargetId) + "|" + str(tupple_target[0][1]) + "|" + str(
                    tupple_target[0][2]) + "|" + ingredientsTarget + "|rdfs:seeAlso|" + str(similarityTotal)

        except:
            print("Error when performing ER")

    else:
        try:
            print("Product has ingredients")
            print(
                "Comparing = " + tupple_source_with_ingredients[0][1] + " with " + tupple_target_with_ingredients[0][1])
            similarityScoreProduct = checkSimilarityJaro(tupple_source_with_ingredients[0][1],
                                                         tupple_target_with_ingredients[0][1])
            print("Similarity product = " + str(similarityScoreProduct))
            print(
                "Comparing = " + tupple_source_with_ingredients[0][2] + " with " + tupple_target_with_ingredients[0][2])
            similarityScoreManufacturer = checkSimilarityJaro(tupple_source_with_ingredients[0][2],
                                                              tupple_target_with_ingredients[0][2])
            print("Similarity manufacturer = " + str(similarityScoreManufacturer))
            print("Comparing ingredients = ")
            similarityScoreIngredients = checkSimilarityCosine(ingredientsSource, ingredientsTarget)
            print("Similarity ingredients = " + str(similarityScoreIngredients))
            similarityTotal = (0.5 * similarityScoreProduct) + (0.2 * similarityScoreManufacturer) + (
                        0.3 * similarityScoreIngredients)
            print("Similarity Total = " + str(similarityTotal))

            if similarityTotal >= threshold:
                print("Gotcha, there is owl:sameAs")
                createOwlSameAsRelationQuery(driver, entitySourceId, entityTargetId)
                isThereOwlSameAs = True
                stringToWrite = str(entitySourceId) + "|" + str(tupple_source_with_ingredients[0][1]) + "|" + str(
                    tupple_source_with_ingredients[0][2]) + "|" + ingredientsSource + "|" + \
                                str(entityTargetId) + "|" + str(tupple_target_with_ingredients[0][1]) + "|" + str(
                    tupple_target_with_ingredients[0][2]) + "|" + ingredientsTarget + "|owl:sameAs|" + str(
                    similarityTotal)
            else:
                createRdfsSeeAlsoRelationQuery(driver, entitySourceId, entityTargetId)
                stringToWrite = str(entitySourceId) + "|" + str(tupple_source_with_ingredients[0][1]) + "|" + str(
                    tupple_source_with_ingredients[0][2]) + "|" + ingredientsSource + "|" + \
                                str(entityTargetId) + "|" + str(tupple_target_with_ingredients[0][1]) + "|" + str(
                    tupple_target_with_ingredients[0][2]) + "|" + ingredientsTarget + "|rdfs:seeAlso|" + str(
                    similarityTotal)

        except:
            print("Error when performing ER")

    listIsThereOwlSameAsAndStringToWrite = [isThereOwlSameAs, stringToWrite]
    return listIsThereOwlSameAsAndStringToWrite


def neo4jgetIdbyLabel(driver, key):
    with driver.session() as session:
        find_entity_query = "MATCH (m:ns1__FoodProduct {rdfs__label: '%s'}) return id(m)" % key

        result = session.run(find_entity_query)
        entity_id = 0
        for r in result:
            entity_id = str(r.value())

        return entity_id


def neo4j_most_similar(driver, model, key):
    with driver.session() as session:
        find_entity_query = "MATCH (m:ns1__FoodProduct {rdfs__label: '%s'}) return id(m)" % key

        result = session.run(find_entity_query)
        for r in result:
            similar_entities = model.most_similar(str(r.value()))
            print("Hasil dari " + key + " with id = " + str(r.value()))
            for s_entity in similar_entities:
                find_entity_query = "MATCH (m:ns1__FoodProduct) where id(m) = %s return m.rdfs__label" % s_entity[0]
                similar_entity_names = session.run(find_entity_query)
                for sm in similar_entity_names:
                    # print the entity name with the cosine similarity
                    print(sm.value() + " with id " + str(s_entity[0]) + " with cousine similarity " + str(s_entity[1]))
                    # print(sm.value() +" : "+str(s_entity[1]))
                    # print(sm.value() +" \\newline")
                    # print(str(s_entity[1]) +" \\newline")


def getEntityDetailsNameAndManuFacture(driver, key):
    with driver.session() as session:
        find_entity_query = "MATCH (m:ns1__FoodProduct)-[:ns3__hasManufacturer]-(other) WHERE id(m) = {} RETURN id(m) AS source, m.rdfs__label AS namaproduk, other.rdfs__label AS target".format(
            key)

        result = session.run(find_entity_query)
        listTupple = result.values()

    return listTupple


def getEntityDetailsNameAndManuFactureAndIngredients(driver, key):
    with driver.session() as session:
        find_entity_query = """
                           MATCH (m:ns1__FoodProduct)-[:ns3__hasManufacturer]-(manufaktur)
                            MATCH (m)-[:ns2__containsIngredient]-(ingredients)
                            WHERE id(m) = {} RETURN id(m) as id, m.rdfs__label as produk, manufaktur.rdfs__label as manufaktur, ingredients.rdfs__label as ingredient 
                            """.format(key)
        result = session.run(find_entity_query)
        listTupple = result.values()

    return listTupple


def neo4j_most_similarById(driver, model, key):
    global similar_entities
    with driver.session() as session:
        find_entity_query = "MATCH (m:ns1__FoodProduct) WHERE id(m) = %s return id(m)" % key
        result = session.run(find_entity_query)
        for r in result:
            similar_entities = model.most_similar(str(r.value()))

    return similar_entities


def checkSimilarityJaccard(str1, str2):
    str1 = str1.lower()
    str2 = str2.lower()
    similarity = textdistance.jaccard.similarity(str1, str2)
    # print("Comparing " + str(str1) + " with " + str(str2)+". Hasil similaritynya = "+str(similarity))
    return similarity


def checkSimilarityJaro(str1, str2):
    str1 = str1.lower()
    str2 = str2.lower()
    similarity = textdistance.jaro_winkler.similarity(str1, str2)
    # print("Comparing " + str(str1) + " with " + str(str2)+". Hasil similaritynya = "+str(similarity))
    return similarity


def checkSimilarityCosine(str1, str2):
    str1 = str1.lower()
    str2 = str2.lower()
    similarity = textdistance.cosine.similarity(str1, str2)

    return similarity


def performEntityResolutionOfEntity(driver, model, entity_id):
    print("Working on entity id = " + str(entity_id))
    listEntities = getEntityProfilesCandidate(driver, model, entity_id)
    # create linkcount to source entity
    createLinkCountProperti(driver, entity_id, len(listEntities))
    print("Number of relevant entities = " + str(len(listEntities)))
    listIsThereOwlSameAsAndStringToWrite = []
    for similarEntityId in listEntities:
        print(
            "Comparing on entity source id = " + str(similarEntityId) + " vs entity target id " + str(similarEntityId))
        isThereOwlSameAsAndStringToWrite = performEntityResolution(driver, entity_id, similarEntityId)
        listIsThereOwlSameAsAndStringToWrite.append(isThereOwlSameAsAndStringToWrite)
    isThereOwlSameAs = False
    for item in listIsThereOwlSameAsAndStringToWrite:
        if item[0] == True:
            isThereOwlSameAs = True
    if isThereOwlSameAs == True:
        print("owl:sameAs was found..")
        for item in listIsThereOwlSameAsAndStringToWrite:
            writecsv("resolutionresultsnew", item[1])
        return True
    else:
        print("owl:sameAs not found.. Skipping write to csv")
        return False


if __name__ == '__main__':

    # uncomment the necessary method based on your task

    host = "bolt://localhost:7687"  # replace this with your neo4j db host
    user = "neo4j"
    password = "123"  # replace this with your neo4j db password

    driver = GraphDatabase.driver(host, auth=(user, password))

    # --Step Creating Edgelist

    # Create edgelist to process in graph embedding product-manufacturer and product-certificates
    # filename = "halalproductmanufacturercertificates"
    # createEdgeListProductManufacturerCertificates(driver, filename)
    # Create edgelist to process in graph embedding product-manufacturer, product-certificates, and product-ingredients
    # filename = "halalproductmanufacturercertificatesingredients"
    # createEdgeListProductManufacturerCertificatesIngrediens(driver, filename)

    # after creating edge list
    # writeEmbedtoNode(driver, filename)

    # --Step Creating Vector Embedding File

    # At this step we wxecute graph embedding process to build .emb embedding file
    # ./node2vec -i:graph/halalcustomnoingredient.edgelist -o:emb/halalcustomnoingredientconf050570.emb -l:70 -q:0.5 -p:0.5 -dr -v
    # command for model 1
    # ./node2vec -i:graph/halalproductmanufacturercertificates.edgelist -o:emb/halalmodel1.emb -q:0.5 -p:0.5 -dr -v
    # command for model 2
    # ./node2vec -i:graph/halalproductmanufacturercertificates.edgelist -o:emb/halalmodel2.emb -l:60 -q:0.5 -p:0.5 -dr -v
    # command for model 3
    # ./node2vec -i:graph/halalproductmanufacturercertificates.edgelist -o:emb/halalmodel3.emb -l:70 -q:0.5 -p:0.5 -dr -v
    # command for model 4
    # ./node2vec -i:graph/halalproductmanufacturercertificatesingredients.edgelist -o:emb/halalmodel4.emb -q:0.5 -p:0.5 -dr -v
    # command for model 5
    # ./node2vec -i:graph/halalproductmanufacturercertificatesingredients.edgelist -o:emb/halalmodel5.emb -l:60 -q:0.5 -p:0.5 -dr -v
    # command for model 6
    # ./node2vec -i:graph/halalproductmanufacturercertificatesingredients.edgelist -o:emb/halalmodel6.emb -l:70 -q:0.5 -p:0.5 -dr -v

    filename = 'emb/halalmodel3.emb'
    model = KeyedVectors.load_word2vec_format(filename, binary=False)

    # --Step Find the most appropriate model
    ##Tes the similar result base on graph embedding process model to get entity profiles candidate,
    # neo4j_most_similar(driver, model, 'Cereal Energen Kacang Hijau')
    # neo4j_most_similar(driver, model, 'Energen rasa kacang hijau')
    # neo4j_most_similar(driver, model, 'Nissin Wafer Krim Coklat Chocolate Wafer Cream ')

    # --Step discovering entity profiles and performing entity resolution task
    # entity_id = neo4jgetIdbyLabel(driver, 'Energen rasa kacang hijau')
    # listEntities = getEntityProfilesCandidate(driver, model, entity_id)
    # print("Number of relevant entities = " + str(len(listEntities)))
    # for similarEntityId in listEntities:
    #     print("Working on entity id = " + str(similarEntityId))
    #     performEntityResolution(driver, model, entity_id, similarEntityId)

    # --Step Discovering entity profiles and performing Entity resolution task All IDs on the embedding vector
    i = 0
    with open(filename, "r") as halal_file, driver.session() as session:
        next(halal_file)
        reader = csv.reader(halal_file, delimiter=" ")
        for row in reader:

            print("Iteration = " + str(i))
            entity_id = row[0]
            isWriteToCsv = performEntityResolutionOfEntity(driver, model, entity_id)
            if isWriteToCsv == True:
                i += 1
            if i == 100:
                break

    ##NOT IMPORTANT
    # Code below is not important, this is just trivial method to test method logic

    ##check similarity
    # skorsimilarity = checkSimilarity('Kopiko L A Coffee As You Like', 'Kopiko Brown Coffee As You Like')
    # print(str(skorsimilarity))

    # string1 = "Wheat Flour, creamer, Calcium carbonate, Sugar, Oat Corn, milk powder, Eggs, Vanili, Salt, Malt Extract"
    # string2 = "Malt Extract, Vitamin Premix, Oat Corn, milk powder, Egg, Salt, Vanili, Wheat Flour, creamer, Calcium carbonate"
    # similaritycosine = textdistance.cosine.similarity(string1,string2)
    # print(str(similaritycosine))

    ##Get entity detail with product name and manufacture
    # data = getEntityDetailsNameAndManuFacture(driver, 15391)
    # print(data[0][2])

    # createOwlSameAsRelationQuery(driver, 23480, 23047)

    # listTupples = getEntityDetailsNameAndManuFactureAndIngredients(driver, 2209)
    # ingredients = ""
    # for tupple in listTupples:
    #     ingredients = ingredients+" "+ tupple[3]

    # print(str(ingredients))
    # if(len(listTupples) == 0):
    #     print("YES")
    # str1 = "John Farmer Peanut Butter Creamy 27"
    # str2 = "John Farmer Peanut Butter Creamy 02"
    # checkSimilarityJaccard(str1, str2)
    # checkSimilarityJaro(str1, str2)