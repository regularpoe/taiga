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

    let createTableCommand = createTableSql tableName schemaPath

    printf "Table '%s' created\n" createTableCommand

    0