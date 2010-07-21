	% BibTeX refdbms family
	% THIS FILE WAS AUTOMATICALLY GENERATED FROM:
	% $Header: /cvs/disc_efficiency/doc/papers/socc10/submit/refabbrv.bst,v 1.1.1.1 2009/12/07 01:49:56 ekrevat Exp $
	% Modifications copyright (C) 1989 John Wilkes,
	%		Hewlett-Packard Company.
	% For BibTeX versions 0.99a or later, LaTeX version 2.09.
	% plain style (sorted numbers)
ENTRY
  { author
    booktitle
    conference
    conflocation
    edition 
    editor
    journal
    key
    note
    number
    pages
    publisher
    reportnumber
    series
    title
    volume
    date
  }
  {}
  { label }
INTEGERS { output.state before.all mid.sentence after.sentence after.block }
INTEGERS {editor.used}
FUNCTION {init.state.consts}
{ #0 'before.all :=
  #1 'mid.sentence :=
  #2 'after.sentence :=
  #3 'after.block :=
}
STRINGS { s t }
FUNCTION {output.nonnull}
{ 's :=
  output.state mid.sentence =
    { ", " * write$ }
    { output.state after.block =
	{ add.period$ write$
	  newline$
	  "\newblock " write$
	}
	{ output.state before.all =
	    'write$
	    { add.period$ " " * write$ }
	  if$
	}
      if$
      mid.sentence 'output.state :=
    }
  if$
  s
}
FUNCTION {output}
{ duplicate$ empty$
    'pop$
    'output.nonnull
  if$
}
FUNCTION {output.check}
{ 't :=
  duplicate$ empty$
    { pop$ "empty " t * " in " * cite$ * warning$ }
    'output.nonnull
  if$
}
FUNCTION {output.bibitem}
{ newline$
  "\bibitem{" write$
  cite$ write$
  "}" write$
  newline$
  ""
  before.all 'output.state :=
  #0 'editor.used :=
}
FUNCTION {fin.entry}
{ add.period$
  write$
  newline$
}
FUNCTION {new.block}
{ output.state before.all =
    'skip$
    { after.block 'output.state := }
  if$
}
FUNCTION {new.sentence}
{ output.state after.block =
    'skip$
    { output.state before.all =
	'skip$
	{ after.sentence 'output.state := }
      if$
    }
  if$
}
FUNCTION {not}
{   { #0 }
    { #1 }
  if$
}
FUNCTION {and}
{   'skip$
    { pop$ #0 }
  if$
}
FUNCTION {or}
{   { pop$ #1 }
    'skip$
  if$
}
FUNCTION {new.block.checka}
{ empty$
    'skip$
    'new.block
  if$
}
FUNCTION {new.block.checkb}
{ empty$
  swap$ empty$
  and
    'skip$
    'new.block
  if$
}
FUNCTION {new.sentence.checka}
{ empty$
    'skip$
    'new.sentence
  if$
}
FUNCTION {new.sentence.checkb}
{ empty$
  swap$ empty$
  and
    'skip$
    'new.sentence
  if$
}
FUNCTION {either.or.check}
{ empty$
    'pop$
    { "can't use both " swap$ * " fields in " * cite$ * warning$ }
  if$
}
FUNCTION {empty.misc.check}
{ author empty$ title empty$
  date empty$ note empty$
  and and and
  key empty$ not and
    { "all relevant fields are empty in " cite$ * warning$ }
    'skip$
  if$
}
FUNCTION {ignoring}
{ empty$
    'pop$
    { "ignoring " swap$ * " in " * cite$ * warning$ }
  if$   
}
FUNCTION {field.or.null}
{ duplicate$ empty$
    { pop$ "" }
    'skip$
  if$
}
FUNCTION {emphasize}
{ duplicate$ empty$
    { pop$ "" }
    { "{\em " swap$ * "}" * }
  if$
}
FUNCTION {embolden}
{ duplicate$ empty$
    { pop$ "" }
    { "{\bf " swap$ * "}" * }
  if$
}
FUNCTION {tie.or.space.connect}
{ duplicate$ text.length$ #3 <
    { "~" }
    { " " }
  if$
  swap$ * *
}
INTEGERS { nameptr namesleft numnames }
FUNCTION {format.names}
{ 's :=
  #1 'nameptr :=
  s num.names$ 'numnames :=
  numnames 'namesleft :=
    { namesleft #0 > }
    { s nameptr "{f.~}{vv~}{ll}{, jj}" format.name$ 't :=
      nameptr #1 >
	{ namesleft #1 >
	    { ", " * t * }
	    { numnames #2 >
		{ "," * }
		'skip$
	      if$
	      t "others" =
		{ " et~al." * }
		{ " and " * t * }
	      if$
	    }
	  if$
	}
	't
      if$
      nameptr #1 + 'nameptr :=
      namesleft #1 - 'namesleft :=
    }
  while$
}
FUNCTION {format.authors}
{ author empty$
    { "" }
    { author format.names }
  if$
}
FUNCTION {format.editors}
{ editor empty$ editor.used or
    { "" }
    { editor format.names
      editor #-1 #1 substring$ ")" =
        'skip$
        { editor num.names$ #1 >
	    { ", editors" * }
	    { ", editor" * }
          if$
        }
      if$
    }
  if$
}
FUNCTION {format.date}
{ date empty$
    { "" }
    { date }
  if$
}
FUNCTION {format.title}
{ title empty$
    { "" }
    'title
  if$
}
FUNCTION {format.btitle}
{ title emphasize
}
FUNCTION {format.rtitle}
{ title emphasize
}
FUNCTION {format.bvolume}
{ volume empty$
    { "" }
    { "volume" volume tie.or.space.connect
      number empty$
	'skip$
	{ ", number" number tie.or.space.connect *
	  "volume+number and series+number" series either.or.check
        }
      if$
      series empty$
	'skip$
	{ " of " * series * }
      if$
    }
  if$
}
FUNCTION {format.number.series}
{ volume empty$
    { number empty$
	{ series field.or.null }
	{ output.state mid.sentence =
	    { "number" }
	    { "Number" }
	  if$
	  number tie.or.space.connect
	  series empty$
	    { "there's a number but no series in " cite$ * warning$ }
	    { " in " * series * }
	  if$
	}
      if$
    }
    { "" }
  if$
}
FUNCTION {format.edition}
{ edition empty$
    { "" }
    { output.state mid.sentence =
        { edition #1 #1 substring$ "l" change.case$
          edition #2 global.max$ substring$ *
        }
	'skip$
      if$
    }
  if$
}
INTEGERS { multiresult }
FUNCTION {multi.page.check}
{ 't :=
  #0 'multiresult :=
    { multiresult not
      t empty$ not
      and
    }
    { t #1 #1 substring$
      duplicate$ "-" =
      swap$ duplicate$ "," =
      swap$ "+" =
      or or
	{ #1 'multiresult := }
	{ t #2 global.max$ substring$ 't := }
      if$
    }
  while$
  multiresult
}
FUNCTION {format.pages}
{ pages empty$
    { "" }
    { output.state mid.sentence =
        { "p" }
	{ "P" }
      if$
      pages multi.page.check
	{ "ages" * pages tie.or.space.connect }
	{ "age"  * pages tie.or.space.connect }
      if$
    }
  if$
}
FUNCTION {format.vol.num.pages}
{ volume field.or.null
  embolden
  number empty$
    'skip$
    { volume empty$
        { number * }
        { "(" number * ")" * * }
      if$
    }
  if$
  pages empty$
    'skip$
    { duplicate$ empty$
	{ pop$ format.pages }
	{ ":" * pages * }
      if$
    }
  if$
}
FUNCTION {format.in.ed.title}
{ field.or.null
  editor empty$
    { "In " }
    { duplicate$ empty$
        { "In " format.editors * }
        { "In " format.editors * ", " * }
      if$
    }
  if$
  swap$ *
}
FUNCTION {format.conference}
{ conference field.or.null
  emphasize
  conflocation empty$
     'skip$
     { " (" conflocation * ")" *  * }
  if$
}
FUNCTION {format.journal}
{ journal empty$
    { "empty journal in " cite$ * warning$ }
    'skip$
  if$
  format.editors output
  journal emphasize output
  format.vol.num.pages
}
FUNCTION {article}
{ output.bibitem
  format.authors "author" output.check
  new.block
  format.title "title" output.check
  new.block
  format.journal output
  publisher conference new.block.checkb
  format.conference output
  publisher output
  format.date "date" output.check
  new.block
  note output
  fin.entry
  "booktitle"	booktitle ignoring
  "edition"	edition	ignoring
  "reportnumber" reportnumber ignoring
  "series"	series	ignoring
}
FUNCTION {book}
{ output.bibitem
  author empty$
    { format.editors "author and editor" output.check }
    { format.authors output.nonnull
      series empty$
        { "author and editor" editor either.or.check }
        'skip$
      if$
    }
  if$
  new.block
  format.btitle "title" output.check
  format.bvolume output
  format.number.series output
  format.editors output
  format.edition output
  publisher reportnumber new.block.checkb
  publisher "publisher" output.check
  reportnumber output
  format.date "date" output.check
  new.block
  note output
  fin.entry
  "booktitle"	booktitle ignoring
  "conference"	conference ignoring
  "conflocation" conflocation ignoring
  "journal"	journal	ignoring
  "pages"	pages	ignoring
}
FUNCTION {inbook}
{ output.bibitem
  format.authors "author" output.check
  new.block
  format.title "title" output.check
  new.block
  booktitle emphasize format.in.ed.title "booktitle" output.check
  format.bvolume output
  format.number.series output
  format.pages "pages" output.check
  format.edition output
  publisher reportnumber new.block.checkb
  publisher "publisher" output.check
  reportnumber output
  format.date "date" output.check
  new.block
  note output
  fin.entry
  "conference"	conference ignoring
  "conflocation" conflocation ignoring
  "journal"	journal	ignoring
}
FUNCTION {inproceedings}
{ output.bibitem
  format.authors "author" output.check
  new.block
  format.title "title" output.check
  new.block
  format.conference "conference and location" output.check
  journal empty$
    { format.bvolume output
      format.number.series output
      format.pages output
      format.editors output
      format.edition output
    }
    { new.block
      "Published as " output
      before.all 'output.state :=
      format.journal output
      "series"	series	ignoring
      "edition"	edition	ignoring
    }
  if$
  publisher reportnumber new.block.checkb
  publisher output
  reportnumber output
  format.date "date" output.check
  new.block
  note output
  fin.entry
  "booktitle"	booktitle ignoring
}
FUNCTION {manual}
{ output.bibitem
  author empty$
  editor empty$
  and
    { publisher empty$
	{ "no author, editor or publisher in " cite$ * warning$ }
	{ publisher output.nonnull }
      if$
    }
    { author empty$
       { format.editors output.nonnull }
       { format.authors output.nonnull }
      if$
    }
  if$
  new.block
  format.btitle "title" output.check
  reportnumber output
  format.edition output
  author empty$
  editor empty$
  and
    'skip$
    { publisher new.block.checka
      publisher output
    }
  if$
  format.date "date" output.check
  new.block
  note output
  fin.entry
  "booktitle"	booktitle ignoring
  "conference"	conference ignoring
  "conflocation" conflocation ignoring
  "journal"	journal	ignoring
  "number"	number	ignoring
  "pages"	pages	ignoring
  "series"	series	ignoring
}
FUNCTION {misc}
{ output.bibitem
  format.authors output
  title new.block.checka
  format.title "title" output.check
  format.editors output
  format.edition output
  publisher reportnumber new.block.checkb
  publisher output
  reportnumber output
  format.date "date" output.check
  new.block
  note "public note" output.check
  fin.entry
  empty.misc.check
  "booktitle"	booktitle ignoring
  "conference"	conference ignoring
  "conflocation" conflocation ignoring
  "journal"	journal	ignoring
  "number"	number	ignoring
  "pages"	pages	ignoring
  "reportnumber" reportnumber ignoring
  "series"	series	ignoring
  "volume"	volume	ignoring
}
FUNCTION {phdthesis}
{ output.bibitem
  format.authors "author" output.check
  new.block
  format.btitle "title" output.check
  new.block
  reportnumber empty$
    { "PhD thesis" output.nonnull }
    { "PhD thesis, published as " reportnumber * output.nonnull }
  if$
  new.block
  publisher "publisher" output.check
  format.date "date" output.check
  new.block
  note output
  fin.entry
  "booktitle"	booktitle ignoring
  "conference"	conference ignoring
  "conflocation" conflocation ignoring
  "edition"	edition	ignoring
  "editor"	editor	ignoring
  "journal"	journal	ignoring
  "number"	number	ignoring
  "pages"	pages	ignoring
  "series"	series	ignoring
  "volume"	volume	ignoring
}
FUNCTION {proceedings}
{ output.bibitem
  author empty$
    { format.editors "author and editor" output.check }
    { "author and editor" editor either.or.check
      format.authors output.nonnull
    }
  if$
  format.btitle "title" output.check
  format.conference output
  journal empty$
    { format.bvolume output
      format.number.series output
      "pages"	pages	ignoring
    }
    { new.block
      "Published as " output.nonnull
      before.all 'output.state :=
      format.journal output
      "series"	series	ignoring
    }
  if$
  publisher reportnumber new.block.checkb
  publisher output
  reportnumber output
  format.date "date" output.check
  new.block
  note output
  fin.entry
  "booktitle"	booktitle ignoring
  "edition"	edition	ignoring
}
FUNCTION {techreport}
{ output.bibitem
  author empty$
    { format.editors "author and editor" output.check }
    { format.authors output.nonnull
      "author and editor" editor either.or.check
    }
  if$
  new.block
  format.rtitle "title" output.check
  new.block
  reportnumber empty$
    { "Technical report" }
    { reportnumber }
  if$
  output.nonnull
  format.edition output
  new.block
  publisher "publisher" output.check
  format.date "date" output.check
  new.block
  note output
  fin.entry
  "booktitle"	booktitle ignoring
  "conference"	conference ignoring
  "conflocation" conflocation ignoring
  "journal"	journal	ignoring
  "number"	number	ignoring
  "pages"	pages	ignoring
  "series"	series	ignoring
  "volume"	volume	ignoring
}
FUNCTION {unpublished}
{ output.bibitem
  format.authors "author" output.check
  new.block
  format.title "title" output.check
  publisher reportnumber new.block.checkb
  publisher output
  reportnumber output
  format.date "date" output.check
  new.block
  note "note" output.check
  fin.entry
  "booktitle"	booktitle ignoring
  "conference"	conference ignoring
  "conflocation" conflocation ignoring
  "edition"	edition	ignoring
  "editor"	editor	ignoring
  "journal"	journal	ignoring
  "number"	number	ignoring
  "pages"	pages	ignoring
  "series"	series	ignoring
  "volume"	volume	ignoring
}
FUNCTION {default.type} { misc }
READ
FUNCTION {sortify}
{ purify$
  "l" change.case$
}
INTEGERS { len }
FUNCTION {chop.word}
{ 's :=
  'len :=
  s #1 len substring$ =
    { s len #1 + global.max$ substring$ }
    's
  if$
}
FUNCTION {sort.format.names}
{ 's :=
  #1 'nameptr :=
  ""
  s num.names$ 'numnames :=
  numnames 'namesleft :=
    { namesleft #0 > }
    { nameptr #1 >
	{ "   " * }
	'skip$
      if$
      s nameptr "{vv{ } }{ll{ }}{  f{ }}{  jj{ }}" format.name$ 't :=
      nameptr numnames = t "others" = and
	{ "et al" * }
	{ t sortify * }
      if$
      nameptr #1 + 'nameptr :=
      namesleft #1 - 'namesleft :=
    }
  while$
}
FUNCTION {sort.format.title}
{ 't :=
  "A " #2
    "An " #3
      "The " #4 t chop.word
    chop.word
  chop.word
  sortify
}
FUNCTION {author.sort}
{ author empty$
    { key empty$
	{ "to sort, need author or key in " cite$ * warning$
	  ""
	}
	{ key sortify }
      if$
    }
    { author sort.format.names }
  if$
}
FUNCTION {author.editor.sort}
{ author empty$
    { editor empty$
	{ key empty$
	    { "to sort, need author, editor, or key in " cite$ * warning$
	      ""
	    }
	    { key sortify }
	  if$
	}
	{ editor sort.format.names }
      if$
    }
    { author sort.format.names }
  if$
}
FUNCTION {author.publisher.sort}
{ author empty$
    { publisher empty$
	{ key empty$
	    { "to sort, need author, publisher, or key in " cite$ * warning$
	      ""
	    }
	    { key sortify }
	  if$
	}
	{ "The " #4 publisher chop.word sortify }
      if$
    }
    { author sort.format.names }
  if$
}
FUNCTION {editor.publisher.sort}
{ editor empty$
    { publisher empty$
	{ key empty$
	    { "to sort, need editor, publisher, or key in " cite$ * warning$
	      ""
	    }
	    { key sortify }
	  if$
	}
	{ "The " #4 publisher chop.word sortify }
      if$
    }
    { editor sort.format.names }
  if$
}
FUNCTION {presort}
{ type$ "book" =
  type$ "inbook" =
  or
    'author.editor.sort
    { type$ "proceedings" =
	'editor.publisher.sort
	{ type$ "manual" =
	    'author.publisher.sort
	    'author.sort
	  if$
	}
      if$
    }
  if$
  "    "
  *
  date field.or.null sortify
  *
  "    "
  *
  title field.or.null
  sort.format.title
  *
  'sort.key$ :=
}
ITERATE {presort}
SORT
STRINGS { longest.label }
INTEGERS { number.label longest.label.width }
FUNCTION {initialize.longest.label}
{ "" 'longest.label :=
  #1 'number.label :=
  #0 'longest.label.width :=
}
FUNCTION {longest.label.pass}
{ number.label int.to.str$ 'label :=
  number.label #1 + 'number.label :=
  label width$ longest.label.width >
    { label 'longest.label :=
      label width$ 'longest.label.width :=
    }
    'skip$
  if$
}
EXECUTE {initialize.longest.label}
ITERATE {longest.label.pass}
FUNCTION {begin.bib}
{ preamble$ empty$
    'skip$
    { preamble$ write$ newline$ }
  if$
  longest.label width$ #0 >		% Original had "9" for "0",
    { "12" 'longest.label := }		% and used "123456789" here
    'skip$
  if$
  "\begin{thebibliography}{"  longest.label  * "}" * write$ newline$
}
EXECUTE {begin.bib}
EXECUTE {init.state.consts}
ITERATE {call.type$}
FUNCTION {end.bib}
{ newline$
  "\end{thebibliography}" write$ newline$
}
EXECUTE {end.bib}
