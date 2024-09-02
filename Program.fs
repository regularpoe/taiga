open System
open System.IO
open Microsoft.Data.Sqlite
open Dapper
open Newtonsoft.Json.Linq

let createTableSql (tableName: String) (schemaFilePath: String) =
    let schema = File.ReadAllText(schemaFilePath) |> JObject.Parse

    let columns =
        schema.Properties()
        |> Seq.map (fun prop ->
            let fieldType =
                match prop.Value.ToString() with
                | "string" -> "TEXT"
                | "int" -> "INTEGER"
                | "float" -> "REAL"
                | "bool" -> "BOOLEAN"
                | _ -> "TEXT"
            sprintf "%s %s" prop.Name fieldType
        )
        |> String.concat ", "

    sprintf "CREATE TABLE IF NOT EXISTS %s (%s);" tableName columns

let createInsertSql (tableName: string) (data: JObject) =
    let columns = data.Properties() |> Seq.map (fun prop -> prop.Name) |> String.concat ", "
    let values = data.Properties() |> Seq.map (fun prop -> sprintf "@%s" prop.Name) |> String.concat ", "
    let sql = sprintf "INSERT INTO %s (%s) VALUES (%s);" tableName columns values
    sql

let jsonToDict (data: JObject) =
    data.Properties()
    |> Seq.map (fun prop -> prop.Name, prop.Value.ToObject(typeof<obj>))
    |> dict

[<EntryPoint>]
let main argv =
    let schemaPath =
        match Array.tryFindIndex (fun x -> x = "-s") argv with
        | Some index when index + 1 < argv.Length -> argv.[index + 1]
        | _ -> failwith "Schema file path not provided"

    let tableName =
        match Array.tryFindIndex (fun x -> x = "-n") argv with
        | Some index when index + 1 < argv.Length -> argv.[index + 1]
        | _ -> failwith "Table name not provdided"

    let dataPath =
        match Array.tryFindIndex (fun x -> x = "-d") argv with
        | Some index when index + 1 < argv.Length -> Some argv.[index + 1]
        | _ -> None

    let connectionString = "Data Source=database.db"

    let createTableCommand = createTableSql tableName schemaPath

    use connection = new SqliteConnection(connectionString)
    connection.Open()

    printfn "Executing SQL: %s" createTableCommand
    connection.Execute(createTableCommand) |> ignore

    match dataPath with
    | Some path ->
        let jsonData = File.ReadAllText(path) |> JArray.Parse
        for item in jsonData do
            let insertCommand = createInsertSql tableName (item :?> JObject)
            let parameters = jsonToDict (item :?> JObject)
            connection.Execute(insertCommand, parameters) |> ignore
            printfn "Inserted data: %s" (item.ToString())
    | None -> 
        printfn "No data file provided, skipping data insertion."

    printfn "Table '%s' created and data inserted successfully." tableName

    0