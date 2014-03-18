package ru.kirkazan.dbsupport;

/**
 * @author esadykov
 * @since 17.03.14 19:23
 */
public class FK
{
    final String name;
    final String fromTable;
    final String fromColumn;
    final String toTable;
    final String toColumn;
    final String matchType;
    final String onUpdate;
    final String onDelete;

    public FK(String name, String fromTable, String fromColumn, String toTable, String toColumn, String matchType, String onUpdate, String onDelete)
    {
        this.name = name;
        this.fromTable = fromTable;
        this.fromColumn = fromColumn;
        this.toTable = toTable;
        this.toColumn = toColumn;
        this.matchType = matchType;
        this.onUpdate = onUpdate;
        this.onDelete = onDelete;
    }

    public boolean isConvenientName()
    {
        return name.equals(getConvenientName());
    }

    public String getConvenientName()
    {
        return fromTable + "__" + fromColumn + "_fk";
    }

    public String getId()
    {
        return fromTable + " " + name;
    }

    public String getConvenientId()
    {
        return fromTable + " " + getConvenientName();
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof FK)) return false;

        FK fk = (FK) o;

        if (!fromColumn.equals(fk.fromColumn)) return false;
        if (!fromTable.equals(fk.fromTable)) return false;
        if (!toColumn.equals(fk.toColumn)) return false;
        if (!toTable.equals(fk.toTable)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = fromTable.hashCode();
        result = 31 * result + fromColumn.hashCode();
        result = 31 * result + toTable.hashCode();
        result = 31 * result + toColumn.hashCode();
        return result;
    }




}

