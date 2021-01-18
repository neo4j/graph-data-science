# frozen_string_literal: true

require 'rubygems'
require 'bundler/setup'
require 'nokogiri'


def parse_children(node)
  return if node.name != 'tocentry'

  filename = find_filename(node)
  children = node
             .children
             .map { |child| parse_children(child) }
             .reject(&:nil?)

  {
    filename: filename,
    children: children
  }
end

def find_filename(node)
  dbthml = node.xpath(".//processing-instruction('dbhtml')").first
  dbthml.content.gsub('filename=', '').delete_prefix('"').delete_suffix('"')
end

def write_content_nav(content_structure, output, depth = 0)
  content_structure.each do |entry|
    output << "#{'*' * (depth + 1)} xref:#{entry[:filename].gsub('html', 'adoc')}[]\n"
    write_content_nav(entry[:children], output, depth + 1)
  end
end

content_map = File.open('docbook/content-map.xml') { |f| Nokogiri::XML(f) }
content_structure = content_map.children[0].children.map { |node| parse_children(node) }.reject(&:nil?)

output_file_name = 'antora/content-nav-generated.adoc'

File.delete(output_file_name) if File.exist?(output_file_name)

File.open(output_file_name, 'w+') do |file|
  write_content_nav(content_structure, file)
end

