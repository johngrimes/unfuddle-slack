require 'bundler/setup'
require 'clockwork'
require 'logger'
require 'net/http'
require 'pg'
require 'slack-notifier'

# Environment variables
# ---
# LOG_LEVEL
# POLL_INTERVAL
# DB_HOST
# DB_PORT
# DB_NAME
# DB_USERNAME
# DB_PASSWORD
# DB_SSL
# UNFUDDLE_SUBDOMAIN
# UNFUDDLE_PROJECT_ID
# UNFUDDLE_USERNAME
# UNFUDDLE_PASSWORD
# MAX_RESULTS
# SLACK_WEBHOOK_URL
# SLACK_CHANNEL
# ICON_URL

module Clockwork
  handler do |job|
    logger = Logger.new(STDOUT)
    logger.level = ENV['LOG_LEVEL'].to_i || Logger::INFO

    # Retrieve last sync time from database.
      db = PG.connect(
        :host => ENV['DB_HOST'],
        :port => ENV['DB_PORT'] || 5432,
        :dbname => ENV['DB_NAME'],
        :user => ENV['DB_USERNAME'],
        :password => ENV['DB_PASSWORD'],
        :sslmode => ENV['DB_SSL'] || 'require'
      )
      logger.info 'Connection to database established.'

    # Ensure we close the database connection when leaving this block.
    begin
      # Ensure that the last_sync_time table exists in the database.
      db.exec(
        'CREATE TABLE IF NOT EXISTS last_sync_time ' +
        '( time TIMESTAMP WITH TIME ZONE NOT NULL )'
      )

      last_sync_time = nil
      db.exec('SELECT time FROM last_sync_time') do |results|
        row_count = results.ntuples
        # There should only ever be one record in this table, we simply update it
        # each time we successfully sync.
        if row_count == 1
          last_sync_time = DateTime.strptime(
            results.first['time'], '%Y-%m-%d %H:%M:%S%z')
          logger.info "Sync time retrieved from database: #{last_sync_time.iso8601}"
        else
          last_sync_time = Time.now
          db.exec_params(
            'INSERT INTO last_sync_time (time) VALUES ($1)',
            [last_sync_time.iso8601]
          )
          logger.warn 'Expected 1 record in last_sync_time table, got ' +
            "#{row_count}. Overriding with the current time, " +
            "#{last_sync_time.iso8601}."
        end
      end

      # Retrieve activity from Unfuddle.
      unfuddle_domain = "#{ENV['UNFUDDLE_SUBDOMAIN']}.unfuddle.com"
      unfuddle_base_url = "https://#{unfuddle_domain}"
      activity_items = []

      Net::HTTP.start(unfuddle_domain, 443, :use_ssl => true) do |http|
        # Format last sync time according to the format accepted by the API method.
        last_sync_time_formatted = last_sync_time.strftime('%a, %d %b %Y %H:%M:%S %Z')

        uri = URI("#{unfuddle_base_url}/api/v1/projects/" +
                  "#{ENV['UNFUDDLE_PROJECT_ID']}/activity.json")
        uri.query = URI.encode_www_form({
          :limit => ENV['MAX_RESULTS'],
          :start_date => last_sync_time_formatted
        })
        logger.debug "uri = #{uri}"
        request = Net::HTTP::Get.new(uri)
        request.basic_auth(ENV['UNFUDDLE_USERNAME'], ENV['UNFUDDLE_PASSWORD'])

        response = http.request(request)
        if response.code == '200'
          activity_items = JSON.parse(response.body)
          logger.info "Unfuddle activity successfully retrieved (start_time = " +
            "\"#{last_sync_time_formatted}\"): #{activity_items.size} new items."
          logger.debug "activity_items = #{activity_items.inspect}"
        else
          logger.error 'Error response from Unfuddle, aborting the rest of the ' +
            "job: #{response.code}"
          return
        end
      end

      # Sort the activity items by date ascending.
      activity_items.sort! { |x,y|
        DateTime.strptime(x['created_at'], '%Y-%m-%dT%H:%M:%S%z') <=>
          DateTime.strptime(y['created_at'], '%Y-%m-%dT%H:%M:%S%z')
      }
      logger.info "Activity items sorted by date ascending."
      logger.debug "activity_items = #{activity_items.inspect}"

      # Ping Slack for each new activity entry.
      slack = Slack::Notifier.new ENV['SLACK_WEBHOOK_URL'],
        :channel => ENV['SLACK_CHANNEL'], :username => 'Unfuddle'
      new_sync_time = last_sync_time

      ping_count = 0
      activity_items.each do |item|
        attachments = []
        if item['record_type'] == 'Ticket'
          attachments = [{
            :fallback => item['record']['ticket']['summary'],
            :title => item['record']['ticket']['summary'],
            :title_link => "#{unfuddle_base_url}/projects/" +
              "#{ENV['UNFUDDLE_PROJECT_ID']}/tickets/by_number/" +
              "#{item['record']['ticket']['number']}",
            :text => item['description'],
            :color => '#ccc'
          }]
        elsif item['record_type'] == 'Comment'
          attachments = [{
            :fallback => item['ticket_summary'],
            :title => item['ticket_summary'],
            :title_link => "#{unfuddle_base_url}/projects/" +
              "#{ENV['UNFUDDLE_PROJECT_ID']}/tickets/by_number/" +
              "#{item['ticket_number']}[comment-#{item['record']['comment']['id']}]",
            :text => item['record']['comment']['body'],
            :color => '#6f6f6f'
          }]
        elsif item['record_type'] == 'Changeset'
          revision = item['record']['changeset']['revision']
          title = "#{item['repository_title']} - #{revision}"
          attachments = [{
            :fallback => title,
            :title => title,
            :title_link => "#{unfuddle_base_url}/a#/repositories/" +
              "#{item['record']['changeset']['repository_id']}/" +
              "commit?commit=#{revision}",
            :text => item['record']['changeset']['message'],
            :color => '#ccc'
          }]
        end
        begin
          slack.ping item['summary'], :attachments => attachments,
            :icon_url => ENV['ICON_URL']
          # Update the new sync time if the ping to Slack was carried out without an
          # error. This way if there is an error, the state will be saved up to that
          # point.
          new_sync_time = DateTime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%S%z')
          ping_count += 1
          logger.info 'Successfully updated Slack with new activity item ' +
            "(#{item['id']}): #{item['summary']}"
          logger.debug "new_sync_time = #{new_sync_time}"
          logger.debug "attachments = #{attachments.inspect}"
        rescue => e
          logger.error "Error updating Slack with activity item (#{item['id']}): " +
            "#{item['summary']}"
          logger.error e.message
          logger.info 'Aborting ping of remaining activity items, new sync time' +
            " will be: #{new_sync_time.iso8601}"
          break
        end
      end
      logger.info "Total #{ping_count} pings to Slack."

      # Update database with new sync time.
      db.exec_params('UPDATE last_sync_time SET time = $1', [new_sync_time])
      logger.info 'Last sync time in database successfully updated to: ' +
        "#{new_sync_time.iso8601}"
    ensure
      db.finish unless db.finished?
      logger.info 'Connection to database closed.'
    end
  end

  every(ENV['POLL_INTERVAL'].to_i, 'unfuddle-slack')
end
