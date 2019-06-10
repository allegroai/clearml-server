# Writing descriptions
There are two options for writing parameters descriptions. Mixing between the two
will result in output which is not Sphinx friendly.
Whatever you choose, lines are subject to wrapping.

- non-strict whitespace - Break the string however you like.
  Newlines and sequences of tabs/spaces are replaced by one space.
  Example:
    ```
    get_all {
        "1.5" {
            description: """This will all appear
            as one long
            sentence.
            Break lines wherever you
            like.
            """
        }
    }
    ```
    Becomes:
    ```
    class GetAllRequest(...):
        """
        This will all appear as one long sentence. Break lines wherever you
        like.
        """
    ```
- strict whitespace - Single newlines will be replaced by spaces.
  Double newlines become a single newline WITH INDENTATION PRESERVED,
  so if uniform indentation is requried for all lines you MUST start new lines
  at the first column.
  Example:
    ```
    get_all {
        "1.5" {
            description: """
    Some general sentence.

    - separate lines must have double newlines between them

    - must begin at first column even though the "description" key is indented

    - you can use single newlines, the lines will be
      joined

        -- sub bullet: this line's leading spaces are preserved
    """
        }
    }
    ```
  Becomes:
    ```
    class GetAllRequest(...):
        """
        Some general sentence.
        - separate lines must have double newlines between them
        - must begin at first column even though the "description" key is indented
        - you can use single newlines, the lines will be joined
            -- sub bullet: this line's leading spaces are preserved
        """
    ```
