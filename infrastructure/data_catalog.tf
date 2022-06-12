resource "aws_glue_catalog_database" "data_lake" {
  name = "${var.environment}-${var.name}-data-lake"
}

resource "aws_glue_catalog_table" "peoples" {
  database_name = aws_glue_catalog_database.data_lake.name
  name          = "peoples"
  parameters    = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/${var.name}/data/peoples/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = false

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "name"
      type = "string"
    }

    columns {
      name = "age"
      type = "int"
    }

    columns {
      name = "gender"
      type = "string"
    }

    columns {
      name = "dept_id"
      type = "string"
    }

    columns {
      name = "salary"
      type = "float"
    }
  }
}

resource "aws_glue_catalog_table" "departments" {
  database_name = aws_glue_catalog_database.data_lake.name
  name          = "departments"
  parameters    = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/${var.name}/data/departments/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = false

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "id"
      type = "string"
    }

    columns {
      name = "name"
      type = "string"
    }

    columns {
      name = "unit"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "wages" {
  database_name = aws_glue_catalog_database.data_lake.name
  name          = "wages"
  parameters    = {
    classification           = "csv"
    delimiter                = ","
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/${var.name}/data/wages/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = false

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"

      parameters = {
        "field.delim"            = ","
        "skip.header.line.count" = "1"
      }
    }

    columns {
      name = "name"
      type = "string"
    }

    columns {
      name = "gender"
      type = "string"
    }

    columns {
      name = "avg_salary"
      type = "double"
    }

    columns {
      name = "max_age"
      type = "int"
    }
  }
}
