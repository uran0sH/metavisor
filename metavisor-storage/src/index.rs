//! Search index implementation using tantivy

use std::path::Path;

use tantivy::{schema::*, Index, IndexReader, IndexWriter};

use crate::error::{Result, StorageError};

/// Search index wrapper
pub struct SearchIndex {
    schema: Schema,
    index: Index,
    reader: IndexReader,
}

impl SearchIndex {
    /// Create a new in-memory search index with the given schema
    pub fn new_memory(schema: Schema) -> Result<Self> {
        let index = Index::create_in_ram(schema.clone());

        let reader = index
            .reader()
            .map_err(|e| StorageError::Index(e.to_string()))?;

        Ok(Self {
            schema,
            index,
            reader,
        })
    }

    /// Create a search index at the given path
    pub fn open<P: AsRef<Path>>(path: P, schema: Schema) -> Result<Self> {
        let index = Index::create_in_dir(path, schema.clone())
            .map_err(|e| StorageError::Index(e.to_string()))?;

        let reader = index
            .reader()
            .map_err(|e| StorageError::Index(e.to_string()))?;

        Ok(Self {
            schema,
            index,
            reader,
        })
    }

    /// Get a writer for adding documents
    pub fn writer(&self) -> Result<IndexWriter> {
        self.index
            .writer(50_000_000)
            .map_err(|e| StorageError::Index(e.to_string()))
    }

    /// Get the reader for searching
    pub fn reader(&self) -> &IndexReader {
        &self.reader
    }

    /// Get the schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get the index
    pub fn index(&self) -> &Index {
        &self.index
    }
}

/// Builder for creating entity search schema
pub fn build_entity_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    // Entity ID (stored and indexed)
    schema_builder.add_text_field("id", STORED | TEXT);

    // Entity type
    schema_builder.add_text_field("type_name", STORED | TEXT);

    // Entity name (for search)
    schema_builder.add_text_field("name", TEXT | STORED);

    // All attributes as JSON (for full-text search)
    schema_builder.add_json_field("attributes", TEXT | STORED);

    // Classifications
    schema_builder.add_text_field("classifications", TEXT | STORED);

    // Created/Updated timestamps
    schema_builder.add_date_field("created", STORED | FAST);
    schema_builder.add_date_field("updated", STORED | FAST);

    schema_builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::doc;

    #[test]
    fn test_create_memory_index() {
        let schema = build_entity_schema();
        let index = SearchIndex::new_memory(schema).unwrap();
        assert!(index.schema().get_field("id").is_ok());
        assert!(index.schema().get_field("type_name").is_ok());
        assert!(index.schema().get_field("name").is_ok());
    }

    #[test]
    fn test_add_and_search_document() {
        let schema = build_entity_schema();
        let index = SearchIndex::new_memory(schema.clone()).unwrap();

        let id_field = schema.get_field("id").unwrap();
        let type_field = schema.get_field("type_name").unwrap();
        let name_field = schema.get_field("name").unwrap();

        let mut writer = index.writer().unwrap();
        writer
            .add_document(doc!(
                id_field => "entity-001",
                type_field => "Table",
                name_field => "users"
            ))
            .unwrap();
        writer.commit().unwrap();

        index.reader().reload().unwrap();

        let searcher = index.reader().searcher();
        let query = tantivy::query::TermQuery::new(
            tantivy::Term::from_field_text(name_field, "users"),
            tantivy::schema::IndexRecordOption::Basic,
        );

        let count = searcher.search(&query, &tantivy::collector::Count).unwrap();
        assert_eq!(count, 1);
    }
}
