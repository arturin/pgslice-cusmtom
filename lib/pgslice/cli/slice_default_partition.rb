# frozen_string_literal: true

module PgSlice
  class CLI
    desc "slice_default_partition TABLE", "Slice out month from default partition if possible"
    option :intermediate, type: :boolean, default: false, desc: "Add to intermediate table"
    option :past, type: :numeric, default: 0, desc: "Number of past partitions to add"
    option :threshold, type: :numeric, default: 0, desc: "Date after which partition for previous month can be created"
    def slice_default_partition(table)
      original_table = create_table(table)
      table = options[:intermediate] ? original_table.intermediate_table : original_table
      default_table = table.default_table
      trigger_name = original_table.trigger_name
      raw_today = Time.now.utc.to_date

      period, field, cast, needs_comment, declarative, version = table.fetch_settings(table.trigger_name)

      unless period
        message = "No settings found: #{table}"
        abort message
      end

      queries = []

      if needs_comment
        queries << "COMMENT ON TRIGGER #{quote_ident(trigger_name)} ON #{quote_table(table)} is 'column:#{field},period:#{period},cast:#{cast}';"
      end

      today = round_date(raw_today, period)

      schema_table =
        if !declarative
          table
        else
          table.partitions.last
        end

      # indexes automatically propagate in Postgres 11+
      if version < 3
        index_defs = schema_table.index_defs
        fk_defs = schema_table.foreign_keys
      else
        index_defs = []
        fk_defs = []
      end

      primary_key = schema_table.primary_key

      added_partitions = []

      past = options[:past]
      range = ((-1 * past)..-1).to_a
      range.delete(-1) if raw_today.day < options[:threshold] # do not account previous month partition if less than n days passed in current month

      range.each do |n|
        day = advance_date(today, period, n)

        partition = Table.new(original_table.schema, "#{original_table.name}_#{day.strftime(name_format(period))}")
        next if partition.exists?
        added_partitions << partition

        if declarative
          queries << <<-SQL
            CREATE TABLE #{quote_table(partition)} PARTITION OF #{quote_table(table)} FOR VALUES FROM (#{sql_date(day, cast, false)}) TO (#{sql_date(advance_date(day, period, 1), cast, false)});
          SQL
        else
          queries << <<-SQL
            CREATE TABLE #{quote_table(partition)}
            (CHECK (#{quote_ident(field)} >= #{sql_date(day, cast)} AND #{quote_ident(field)} < #{sql_date(advance_date(day, period, 1), cast)}))
            INHERITS (#{quote_table(table)});
          SQL
        end

        queries << "ALTER TABLE #{quote_table(partition)} ADD PRIMARY KEY (#{primary_key.map { |k| quote_ident(k) }.join(", ")});" if primary_key.any?

        index_defs.each do |index_def|
          queries << make_index_def(index_def, partition)
        end

        fk_defs.each do |fk_def|
          queries << make_fk_def(fk_def, partition)
        end
      end

      unless declarative
        # update trigger based on existing partitions
        past_defs = []
        name_format = self.name_format(period)
        partitions = (table.partitions + added_partitions).uniq(&:name).sort_by(&:name)

        partitions.each do |partition|
          day = partition_date(partition, name_format)

          sql = "(NEW.#{quote_ident(field)} >= #{sql_date(day, cast)} AND NEW.#{quote_ident(field)} < #{sql_date(advance_date(day, period, 1), cast)}) THEN
            INSERT INTO #{quote_table(partition)} VALUES (NEW.*);"

          if day.to_date < today
            past_defs << sql
          end
        end

        # order by current period, future periods asc, past periods desc
        trigger_defs = past_defs.reverse

        if trigger_defs.any?
          queries << <<-SQL
            CREATE OR REPLACE FUNCTION #{quote_ident(trigger_name)}()
            RETURNS trigger AS $$
            BEGIN
              IF #{trigger_defs.join("\n        ELSIF ")}
              ELSE
                RAISE EXCEPTION 'Date out of range. Ensure partitions are created.';
              END IF;
            RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
          SQL
        end
      end

      # move entities from default partition to partition
      if added_partitions.any?
        name_format = self.name_format(period)
        starting_time = partition_date(added_partitions.first, name_format)
        ending_time = advance_date(partition_date(added_partitions.last, name_format), period, 1)

        max_dest_id = table.partitions[-2].max_id(primary_key)
        log_sql "max_dest_id: #{max_dest_id}"
        max_source_id = default_table.max_id(primary_key)
        log_sql "max_source_id: #{max_source_id}"
        starting_id = max_dest_id
        fields = table.columns.map { |c| quote_ident(c) }.join(", ")
        batch_size = 10000
        i = 1
        batch_count = ((max_source_id - max_dest_id) / batch_size.to_f).ceil

        if batch_count == 0
          log_sql "/* nothing to fill */"
        end

        while starting_id < max_source_id
          where = "#{quote_ident(primary_key)} > #{starting_id} AND #{quote_ident(primary_key)} <= #{starting_id + batch_size}"
          if starting_time
            where += " AND #{quote_ident(field)} >= #{sql_date(starting_time, cast)} AND #{quote_ident(field)} < #{sql_date(ending_time, cast)}"
          end

          queries << <<-SQL
            /* #{i} of #{batch_count} */
            INSERT INTO #{quote_table(table)} (#{fields})
            SELECT #{fields} FROM #{quote_table(default_table)}
            WHERE #{where}
          SQL

          queries << <<-SQL
            DELETE FROM #{quote_table(default_table)}
            WHERE #{where}
          SQL

          starting_id += batch_size
          i += 1
        end
      end

      if queries.any?
        if default_table.exists?
          # detach default_table
          queries.unshift("ALTER TABLE IF EXISTS #{table.quote_table} DETACH PARTITION #{default_table.quote_table};")
          # attach default partition
          queries << <<-SQL
            ALTER TABLE IF EXISTS #{table.quote_table} ATTACH PARTITION #{default_table.quote_table} DEFAULT;
          SQL
        end

        queries.unshift("LOCK TABLE #{table.quote_table} IN ACCESS SHARE MODE;")

        run_queries(queries)
      end
    end
  end
end
