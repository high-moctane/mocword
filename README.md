# mocword

Predict next English words (｀･ω･´)

## Usage

### One shot mode

```
$ mocword -q "this is "
the
a
not
an
what
to
that
true
done
so
```

```
$ mocword -q "one of t" --limit 3
the
them
these
```

### Interactive mode

```
$ mocword
this is
the a not an what to that true done so
one of t_
the them these those their two three this that themselves
```

The underscore is a white space.

## Query string

Ends with a white space -> predict successive words.

Ends without any white space -> predict words which begin with the last word's prefix.

## License

MIT
